"""Consulta de clusters H3 compartilhada pela Lambda pública e pelo servidor local."""
import base64
import json
import os
from datetime import datetime, timezone

import boto3
import h3
from boto3.dynamodb.conditions import Key

CLUSTER_PK = 'CLUSTER'


def query_clusters(satellite=None, cursor=None, limit=500):
    if satellite is not None and (not isinstance(satellite, str) or len(satellite) > 100):
        raise ValueError('Satélite inválido')
    if isinstance(limit, bool) or not isinstance(limit, int) or not 1 <= limit <= 1000:
        raise ValueError('limit deve estar entre 1 e 1000')
    request = {
        'KeyConditionExpression': Key('pk').eq(CLUSTER_PK),
        'Limit': limit,
        'ConsistentRead': True,
    }
    if cursor:
        try:
            if not isinstance(cursor, str) or len(cursor) > 4096:
                raise ValueError()
            start = json.loads(base64.b64decode(cursor, altchars=b'-_', validate=True))
            if (not isinstance(start, dict) or set(start) != {'pk', 'sk'}
                    or start['pk'] != CLUSTER_PK or not isinstance(start['sk'], str)
                    or not start['sk'].startswith('H3#')):
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
        features.append({
            'type': 'Feature',
            'geometry': {'type': 'Polygon', 'coordinates': [ring]},
            'properties': {
                'h3': item['h3'],
                'satellite': item['satellite'],
                'count': int(item['count']),
            },
        })
    last = result.get('LastEvaluatedKey')
    return {
        'type': 'FeatureCollection',
        'features': features,
        'next_cursor': base64.urlsafe_b64encode(json.dumps(last).encode()).decode() if last else None,
        'queried_at': datetime.now(timezone.utc).isoformat(),
    }


def clear_clusters():
    """Apaga todos os itens da tabela DynamoDB usando scan de chaves + batch_writer."""
    table = boto3.resource('dynamodb').Table(os.environ['TABLE_NAME'])
    deleted = 0
    scan_kwargs = {'ProjectionExpression': 'pk, sk'}
    while True:
        page = table.scan(**scan_kwargs)
        items = page.get('Items', [])
        if items:
            with table.batch_writer() as batch:
                for item in items:
                    batch.delete_item(Key={'pk': item['pk'], 'sk': item['sk']})
                    deleted += 1
        last = page.get('LastEvaluatedKey')
        if not last:
            break
        scan_kwargs['ExclusiveStartKey'] = last
    return {'deleted': deleted, 'cleared_at': datetime.now(timezone.utc).isoformat()}
