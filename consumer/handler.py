"""Consumidor SQS FIFO: agrega cada observação por janela de 10 minutos UTC, satélite e H3."""
import json
import logging
import os
from datetime import datetime, timezone

import boto3
import h3
from boto3.dynamodb.types import TypeSerializer
from botocore.exceptions import ClientError

from common.records import AGGREGATION, record, ten_minute_window

LOG = logging.getLogger(__name__)
LOG.setLevel(logging.INFO)
SERIALIZER = TypeSerializer()


def encode(item):
    return {k: SERIALIZER.serialize(v) for k, v in item.items()}


def persist(client, table, item, resolution):
    canonical = record(item['date'], item['satellite'], item['latitude'], item['longitude'])
    if item.get('id') != canonical['id']:
        raise ValueError('Identificador do registro não corresponde ao conteúdo')
    cell = h3.latlng_to_cell(canonical['latitude'], canonical['longitude'], resolution)
    start, end = ten_minute_window(canonical['date'])
    day = start[:10]
    marker = {'pk': f"EVENT#{canonical['id']}", 'sk': f'RES#{resolution}#{AGGREGATION}', **canonical, 'h3': cell}
    # Floats não são aceitos pelo serializer DynamoDB.
    from decimal import Decimal
    marker['latitude'] = Decimal(str(canonical['latitude']))
    marker['longitude'] = Decimal(str(canonical['longitude']))
    cluster = {'pk': f'DAY#{day}', 'sk': f"RES#{resolution}#{AGGREGATION}#{start}#H3#{cell}#SAT#{canonical['satellite']}"}
    now = datetime.now(timezone.utc).isoformat()
    try:
        client.transact_write_items(TransactItems=[
            {'Put': {'TableName': table, 'Item': encode(marker), 'ConditionExpression': 'attribute_not_exists(pk)'}},
            {'Update': {'TableName': table, 'Key': encode(cluster),
                        'UpdateExpression': 'SET #d=:d, window_end=:end, satellite=:s, h3=:h, resolution=:r, updated_at=:u ADD #n :one',
                        'ExpressionAttributeNames': {'#d': 'date', '#n': 'count'},
                        'ExpressionAttributeValues': encode({':d': start, ':end': end, ':s': canonical['satellite'], ':h': cell,
                                                            ':r': resolution, ':u': now, ':one': 1})}}
        ])
        return True
    except ClientError as error:
        if error.response['Error']['Code'] == 'TransactionCanceledException':
            reasons = error.response.get('CancellationReasons', [])
            if reasons and reasons[0].get('Code') == 'ConditionalCheckFailed':
                return False  # marcador e contador são gravados na mesma transação
        raise


def handler(event, context):
    client = boto3.client('dynamodb')
    table = os.environ['TABLE_NAME']
    resolution = int(os.getenv('H3_RESOLUTION', '5'))
    if not 0 <= resolution <= 15:
        raise ValueError('Resolução H3 deve estar entre 0 e 15')
    messages = event.get('Records', [])
    for index, message in enumerate(messages):
        try:
            payload = json.loads(message['body'])
            records = payload['records']
            if payload.get('schema_version') != 1 or not isinstance(records, list) or not 1 <= len(records) <= 10:
                raise ValueError('Mensagem deve conter schema_version=1 e de 1 a 10 registros')
            for item in records:
                persist(client, table, item, resolution)
        except Exception:
            LOG.exception('Falha na mensagem %s', message['messageId'])
            # FIFO: interromper e devolver também as mensagens seguintes, preservando ordem.
            return {'batchItemFailures': [{'itemIdentifier': m['messageId']} for m in messages[index:]]}
    return {'batchItemFailures': []}
