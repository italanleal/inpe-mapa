"""Integração AWS simulada: instalar requirements-dev.txt para executar."""
import json

import pytest
boto3 = pytest.importorskip('boto3')
h3 = pytest.importorskip('h3')
moto = pytest.importorskip('moto')
pytest.importorskip('fastapi')
pytest.importorskip('httpx')
from fastapi.testclient import TestClient
from backend.app import app
from common.records import record
from consumer.handler import handler


@pytest.fixture
def aws(monkeypatch):
    monkeypatch.setenv('AWS_ACCESS_KEY_ID', 'test')
    monkeypatch.setenv('AWS_SECRET_ACCESS_KEY', 'test')
    monkeypatch.setenv('AWS_DEFAULT_REGION', 'us-east-1')
    monkeypatch.setenv('TABLE_NAME', 'test-clusters')
    monkeypatch.setenv('H3_RESOLUTION', '5')
    with moto.mock_aws():
        client = boto3.client('dynamodb')
        client.create_table(
            TableName='test-clusters',
            BillingMode='PAY_PER_REQUEST',
            KeySchema=[{'AttributeName': 'pk', 'KeyType': 'HASH'}, {'AttributeName': 'sk', 'KeyType': 'RANGE'}],
            AttributeDefinitions=[{'AttributeName': 'pk', 'AttributeType': 'S'}, {'AttributeName': 'sk', 'AttributeType': 'S'}],
        )
        yield client


def event(items):
    return {'Records': [{'messageId': 'm1', 'body': json.dumps({'schema_version': 1, 'records': items})}]}


def test_collisions_increment_count_and_store_minimal_fields(aws):
    first = record('2026-11-10T01:00:00Z', 'AQUA', -10, -50)
    second = record('2026-11-11T05:30:00Z', 'AQUA', -10, -50)
    assert handler(event([first]), None) == {'batchItemFailures': []}
    assert handler(event([second]), None) == {'batchItemFailures': []}
    rows = aws.scan(TableName='test-clusters')['Items']
    assert len(rows) == 1
    cluster = rows[0]
    assert set(cluster) == {'pk', 'sk', 'h3', 'satellite', 'count'}
    assert cluster['pk']['S'] == 'CLUSTER'
    assert cluster['count']['N'] == '2'
    assert cluster['satellite']['S'] == 'AQUA'
    assert cluster['h3']['S'] == h3.latlng_to_cell(-10, -50, 5)


def test_geojson_pagination_and_satellite_filter(aws):
    first = record('2026-11-10T01:00:00Z', 'AQUA', -10, -50)
    second = record('2026-11-10T02:00:00Z', 'TERRA', -10, -50)
    assert handler(event([first, second]), None)['batchItemFailures'] == []
    with TestClient(app) as api:
        page = api.get('/api/clusters', params={'limit': 1}).json()
        assert page['next_cursor']
        next_page = api.get('/api/clusters', params={'limit': 1, 'cursor': page['next_cursor']}).json()
        features = page['features'] + next_page['features']
        assert sum(f['properties']['count'] for f in features) == 2
        assert set(features[0]['properties']) == {'h3', 'satellite', 'count'}
        ring = features[0]['geometry']['coordinates'][0]
        assert ring[0] == ring[-1]
        assert ring[0][0] < -40  # longitude antes de latitude
        assert api.get('/api/clusters?cursor=bad').status_code == 400
        assert api.get('/').status_code == 200


def test_fifo_stops_at_first_failure(aws):
    item = record('2026-11-10', 'AQUA', -10, -50)
    response = handler({'Records': [{'messageId': 'bad', 'body': '{}'}, event([item])['Records'][0]]}, None)
    assert response['batchItemFailures'] == [{'itemIdentifier': 'bad'}, {'itemIdentifier': 'm1'}]
    assert aws.scan(TableName='test-clusters')['Count'] == 0


def test_function_url_with_real_query(aws):
    from backend.handler import handler as api_handler
    item = record('2026-11-10T15:41:00Z', 'AQUA', -10, -50)
    assert handler(event([item]), None)['batchItemFailures'] == []
    result = api_handler({
        'version': '2.0',
        'rawPath': '/api/clusters',
        'requestContext': {'http': {'method': 'GET'}},
        'queryStringParameters': {'satellite': 'AQUA'},
    }, None)
    assert result['statusCode'] == 200
    assert json.loads(result['body'])['features'][0]['properties'] == {
        'h3': h3.latlng_to_cell(-10, -50, 5),
        'satellite': 'AQUA',
        'count': 1,
    }
    cleared = api_handler({
        'version': '2.0',
        'rawPath': '/api/clusters',
        'requestContext': {'http': {'method': 'DELETE'}},
    }, None)
    assert cleared['statusCode'] == 200
    assert json.loads(cleared['body'])['deleted'] == 1
    assert aws.scan(TableName='test-clusters')['Count'] == 0
