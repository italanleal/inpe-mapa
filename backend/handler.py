"""Endpoint HTTP público para Lambda Function URL (payload 2.0)."""
import json
import logging
from datetime import datetime, timezone

from botocore.exceptions import BotoCoreError, ClientError
from backend.query import query_clusters, query_observations

LOG = logging.getLogger(__name__)


def response(status, body):
    # CORS pertence à configuração da Function URL, evitando cabeçalhos duplicados.
    return {'statusCode': status, 'headers': {'Content-Type': 'application/json; charset=utf-8',
                                            'Cache-Control': 'no-store'},
            'isBase64Encoded': False, 'body': json.dumps(body, ensure_ascii=False)}


def handler(event, context):
    method = event.get('requestContext', {}).get('http', {}).get('method', '')
    if method != 'GET':
        result = response(405, {'detail': 'Método não permitido'})
        result['headers']['Allow'] = 'GET'
        return result
    path = event.get('rawPath', '/')
    if path in ('/', '/api/health'):
        return response(200, {'status': 'ok', 'time': datetime.now(timezone.utc).isoformat()})
    if path != '/api/clusters':
        return response(404, {'detail': 'Rota não encontrada'})
    params = event.get('queryStringParameters') or {}
    try:
        if not isinstance(params, dict):
            raise ValueError('Parâmetros inválidos')
        try:
            limit = int(params.get('limit', '500'))
        except (ValueError, TypeError):
            raise ValueError('limit deve ser um número inteiro') from None
        if 'day' in params and 'view' not in params:
            data = query_clusters(day=params.get('day'), satellite=params.get('satellite'),
                                  window=params.get('window'), cursor=params.get('cursor'), limit=limit)
        else:
            try:
                minutes = int(params['minutes']) if 'minutes' in params else None
            except (ValueError, TypeError):
                raise ValueError('minutes deve ser um número inteiro') from None
            data = query_observations(view=params.get('view', 'all'), minutes=minutes,
                                      satellite=params.get('satellite'), cursor=params.get('cursor'),
                                      limit=limit, as_of=params.get('as_of'))
        return response(200, data)
    except ValueError as error:
        return response(400, {'detail': str(error)})
    except (BotoCoreError, ClientError):
        LOG.exception('Falha consultando DynamoDB')
        return response(503, {'detail': 'Consulta indisponível temporariamente'})
    except Exception:
        LOG.exception('Erro interno na consulta')
        return response(500, {'detail': 'Erro interno'})
