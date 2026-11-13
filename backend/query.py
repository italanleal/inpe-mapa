"""Consulta de agregados compartilhada pela Lambda pública e pelo servidor local."""
import base64
import json
import os
import re
from datetime import date, datetime, timezone

import boto3
import h3
from boto3.dynamodb.conditions import Key

from common.records import AGGREGATION


def query_clusters(day, satellite=None, window=None, cursor=None, limit=500):
    try:
        parsed_day = date.fromisoformat(day)
        if parsed_day.isoformat() != day:
            raise ValueError()
    except (ValueError, TypeError):
        raise ValueError('day deve estar no formato YYYY-MM-DD') from None
    if satellite is not None and (not isinstance(satellite, str) or len(satellite) > 100):
        raise ValueError('Satélite inválido')
    if window is not None and (not isinstance(window, str) or not re.fullmatch(r'(?:[01][0-9]|2[0-3]):[0-5]0', window)):
        raise ValueError('window deve ser HH:MM em UTC, em múltiplos de 10 minutos')
    if isinstance(limit, bool) or not isinstance(limit, int) or not 1 <= limit <= 1000:
        raise ValueError('limit deve estar entre 1 e 1000')
    resolution = int(os.getenv('H3_RESOLUTION', '5'))
    prefix = f'RES#{resolution}#{AGGREGATION}#'
    if window:
        prefix += f'{day}T{window}:00Z#'
    request = {'KeyConditionExpression': Key('pk').eq(f'DAY#{day}') & Key('sk').begins_with(prefix),
               'Limit': limit, 'ConsistentRead': True}
    if cursor:
        try:
            if not isinstance(cursor, str) or len(cursor) > 4096:
                raise ValueError()
            start = json.loads(base64.b64decode(cursor, altchars=b'-_', validate=True))
            if (not isinstance(start, dict) or set(start) != {'pk', 'sk'} or start['pk'] != f'DAY#{day}'
                    or not isinstance(start['sk'], str) or not start['sk'].startswith(prefix)):
                raise ValueError()
            request['ExclusiveStartKey'] = start
        except Exception:
            raise ValueError('Cursor inválido para esta consulta') from None
    table = boto3.resource('dynamodb').Table(os.environ['TABLE_NAME'])
    result = table.query(**request)
    features = []
    for item in result.get('Items', []):
        if satellite and item['satellite'] != satellite.upper():
            continue
        ring = [[lng, lat] for lat, lng in h3.cell_to_boundary(item['h3'])]
        ring.append(ring[0])
        features.append({'type': 'Feature', 'geometry': {'type': 'Polygon', 'coordinates': [ring]},
                         'properties': {'date': item['date'], 'window_end': item['window_end'],
                                        'satellite': item['satellite'], 'h3': item['h3'],
                                        'count': int(item['count']), 'resolution': int(item['resolution']),
                                        'updated_at': item['updated_at']}})
    last = result.get('LastEvaluatedKey')
    return {'type': 'FeatureCollection', 'features': features,
            'next_cursor': base64.urlsafe_b64encode(json.dumps(last).encode()).decode() if last else None,
            'queried_at': datetime.now(timezone.utc).isoformat()}


def query_observations(view='all', minutes=None, satellite=None, cursor=None, limit=500, as_of=None):
    """Lê o índice das observações e agrega cada página por H3/satélite.

    Todas as páginas usam o mesmo as_of; a interface soma células entre páginas.
    O índice é eventualmente consistente, como todo GSI DynamoDB.
    """
    from datetime import timedelta
    from common.records import timestamp
    if view not in ('all', 'recent'):
        raise ValueError('view deve ser all ou recent')
    if view == 'recent' and (isinstance(minutes, bool) or not isinstance(minutes, int) or not 1 <= minutes <= 525600):
        raise ValueError('minutes deve estar entre 1 e 525600')
    if view == 'all' and minutes is not None:
        raise ValueError('minutes exige view=recent')
    if satellite is not None and (not isinstance(satellite, str) or len(satellite) > 100):
        raise ValueError('Satélite inválido')
    if isinstance(limit, bool) or not isinstance(limit, int) or not 1 <= limit <= 1000:
        raise ValueError('limit deve estar entre 1 e 1000')
    now = datetime.now(timezone.utc)
    if as_of is not None:
        try:
            if not isinstance(as_of, str) or len(as_of) > 40 or datetime.fromisoformat(as_of.replace('Z', '+00:00')).tzinfo is None:
                raise ValueError()
            reference = datetime.fromisoformat(timestamp(as_of).replace('Z', '+00:00'))
            if reference > now + timedelta(seconds=30):
                raise ValueError()
        except (ValueError, TypeError):
            raise ValueError('as_of deve ser um instante UTC válido, não futuro') from None
    else:
        reference = now
    reference_time = timestamp(reference.isoformat())
    cutoff = timestamp((reference - timedelta(minutes=minutes)).isoformat()) if view == 'recent' else None
    resolution = int(os.getenv('H3_RESOLUTION', '5'))
    marker_sk = f'RES#{resolution}#{AGGREGATION}'
    condition = Key('sk').eq(marker_sk)
    if cutoff:
        condition &= Key('date').between(cutoff, reference_time)
    request = {'IndexName': os.getenv('OBSERVATIONS_INDEX', 'ObservationsByTime'),
               'KeyConditionExpression': condition, 'Limit': limit}
    # O cursor carrega o contexto temporal para evitar misturar janelas entre páginas.
    context = {'view': view, 'minutes': minutes, 'satellite': satellite, 'resolution': resolution}
    if cursor:
        try:
            if not isinstance(cursor, str) or len(cursor) > 4096:
                raise ValueError()
            token = json.loads(base64.b64decode(cursor, altchars=b'-_', validate=True))
            if token['context'] != context or token['as_of'] != reference_time:
                raise ValueError()
            key = token['key']
            if set(key) != {'pk', 'sk', 'date'} or key['sk'] != marker_sk or not isinstance(key['pk'], str) or not key['pk'].startswith('EVENT#') or not isinstance(key['date'], str):
                raise ValueError()
            request['ExclusiveStartKey'] = key
        except Exception:
            raise ValueError('Cursor inválido; preserve as_of e os filtros entre páginas') from None
    table = boto3.resource('dynamodb').Table(os.environ['TABLE_NAME'])
    result = table.query(**request)
    grouped = {}
    for item in result.get('Items', []):
        if satellite and item['satellite'] != satellite.upper():
            continue
        cell, sat = item['h3'], item['satellite']
        key = (cell, sat)
        if key not in grouped:
            ring = [[lng, lat] for lat, lng in h3.cell_to_boundary(cell)]
            ring.append(ring[0])
            grouped[key] = {'type': 'Feature', 'geometry': {'type': 'Polygon', 'coordinates': [ring]},
                            'properties': {'h3': cell, 'satellite': sat, 'resolution': resolution, 'count': 0,
                                           'first_observed_at': item['date'], 'last_observed_at': item['date']}}
        props = grouped[key]['properties']
        props['count'] += 1
        props['first_observed_at'] = min(props['first_observed_at'], item['date'])
        props['last_observed_at'] = max(props['last_observed_at'], item['date'])
    last = result.get('LastEvaluatedKey')
    token = {'key': last, 'context': context, 'as_of': reference_time}
    return {'type': 'FeatureCollection', 'features': list(grouped.values()),
            'next_cursor': base64.urlsafe_b64encode(json.dumps(token).encode()).decode() if last else None,
            'reference_time': reference_time, 'cutoff': cutoff, 'view': view,
            'queried_at': now.isoformat()}
