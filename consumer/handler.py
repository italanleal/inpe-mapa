"""Consumidor SQS FIFO: agrega cada observação diretamente por H3 e satélite no DynamoDB."""

import json
import logging
import os

import boto3
import h3
from boto3.dynamodb.types import TypeSerializer

from common.records import record

LOG = logging.getLogger(__name__)
LOG.setLevel(logging.INFO)
SERIALIZER = TypeSerializer()
CLUSTER_PK = 'CLUSTER'


def encode(item):
    return {k: SERIALIZER.serialize(v) for k, v in item.items()}


def persist(client, table, item, resolution):
    canonical = record(item['date'], item['satellite'], item['latitude'], item['longitude'])
    if item.get('id') != canonical['id']:
        raise ValueError('Identificador do registro não corresponde ao conteúdo')
    cell = h3.latlng_to_cell(canonical['latitude'], canonical['longitude'], resolution)
    key = {'pk': CLUSTER_PK, 'sk': f"H3#{cell}#SAT#{canonical['satellite']}"}
    client.update_item(
        TableName=table,
        Key=encode(key),
        UpdateExpression='SET h3 = :h, satellite = :s ADD #n :one',
        ExpressionAttributeNames={'#n': 'count'},
        ExpressionAttributeValues=encode({':h': cell, ':s': canonical['satellite'], ':one': 1}),
    )
    return True


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
