"""Endpoint HTTP público para Lambda Function URL (payload 2.0)."""
import json
import logging
from datetime import datetime, timezone

from botocore.exceptions import BotoCoreError, ClientError
from backend.query import clear_clusters, query_clusters

LOG = logging.getLogger(__name__)


def response(status, body):
    # CORS pertence à configuração da Function URL, evitando cabeçalhos duplicados.
    return {
        'statusCode': status,
        'headers': {'Content-Type': 'application/json; charset=utf-8', 'Cache-Control': 'no-store'},
        'isBase64Encoded': False,
        'body': json.dumps(body, ensure_ascii=False),
    }


def handler(event, context):
    method = event.get('requestContext', {}).get('http', {}).get('method', '')
    if method not in ('GET', 'DELETE'):
        result = response(405, {'detail': 'Método não permitido'})
        result['headers']['Allow'] = 'GET, DELETE'
        return result
    path = event.get('rawPath', '/')
    if method == 'GET' and path in ('/', '/api/health'):
        return response(200, {'status': 'ok', 'time': datetime.now(timezone.utc).isoformat()})
    if path != '/api/clusters':
        return response(404, {'detail': 'Rota não encontrada'})
    try:
        if method == 'DELETE':
            return response(200, clear_clusters())
        params = event.get('queryStringParameters') or {}
        if not isinstance(params, dict):
            raise ValueError('Parâmetros inválidos')
        try:
            limit = int(params.get('limit', '500'))
        except (ValueError, TypeError):
            raise ValueError('limit deve ser um número inteiro') from None
        data = query_clusters(
            satellite=params.get('satellite'),
            cursor=params.get('cursor'),
            limit=limit,
        )
        return response(200, data)
    except ValueError as error:
        return response(400, {'detail': str(error)})
    except (BotoCoreError, ClientError):
        LOG.exception('Falha acessando DynamoDB')
        return response(503, {'detail': 'Operação no banco indisponível temporariamente'})
    except Exception:
        LOG.exception('Erro interno na API')
        return response(500, {'detail': 'Erro interno'})
