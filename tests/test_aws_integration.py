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
from consumer.handler import handler, persist


@pytest.fixture
def aws(monkeypatch):
    monkeypatch.setenv('AWS_ACCESS_KEY_ID', 'test')
    monkeypatch.setenv('AWS_SECRET_ACCESS_KEY', 'test')
    monkeypatch.setenv('AWS_DEFAULT_REGION', 'us-east-1')
    monkeypatch.setenv('TABLE_NAME', 'test-clusters')
    monkeypatch.setenv('H3_RESOLUTION', '5')
    with moto.mock_aws():
        client = boto3.client('dynamodb')
        client.create_table(TableName='test-clusters', BillingMode='PAY_PER_REQUEST',
                            KeySchema=[{'AttributeName': 'pk', 'KeyType': 'HASH'}, {'AttributeName': 'sk', 'KeyType': 'RANGE'}],
                            AttributeDefinitions=[{'AttributeName': 'pk', 'AttributeType': 'S'}, {'AttributeName': 'sk', 'AttributeType': 'S'}, {'AttributeName': 'date', 'AttributeType': 'S'}],
                            GlobalSecondaryIndexes=[{'IndexName': 'ObservationsByTime', 'KeySchema': [
                                {'AttributeName': 'sk', 'KeyType': 'HASH'}, {'AttributeName': 'date', 'KeyType': 'RANGE'}],
                                'Projection': {'ProjectionType': 'ALL'}}])
        yield client


def event(items):
    return {'Records': [{'messageId': 'm1', 'body': json.dumps({'schema_version': 1, 'records': items})}]}


def test_duplicate_retry_does_not_increment(aws):
    item = record('2026-11-10T01:00:00Z', 'AQUA', -10, -50)
    assert handler(event([item]), None) == {'batchItemFailures': []}
    assert handler(event([item]), None) == {'batchItemFailures': []}
    rows = aws.scan(TableName='test-clusters')['Items']
    cluster = next(r for r in rows if r['pk']['S'].startswith('DAY#'))
    assert cluster['count']['N'] == '1'
    assert cluster['h3']['S'] == h3.latlng_to_cell(-10, -50, 5)
    assert len(rows) == 2


def test_partial_message_retry_and_geojson_pagination(aws):
    first = record('2026-11-10T01:00:00Z', 'AQUA', -10, -50)
    second = record('2026-11-10T02:00:00Z', 'TERRA', -10, -50)
    bad = {**second, 'latitude': 999}
    assert handler(event([first, bad]), None)['batchItemFailures'] == [{'itemIdentifier': 'm1'}]
    assert handler(event([first, second]), None)['batchItemFailures'] == []
    with TestClient(app) as api:
        page = api.get('/api/clusters', params={'day': '2026-11-10', 'limit': 1}).json()
        assert page['next_cursor']
        next_page = api.get('/api/clusters', params={'day': '2026-11-10', 'limit': 1, 'cursor': page['next_cursor']}).json()
        features = page['features'] + next_page['features']
        assert sum(f['properties']['count'] for f in features) == 2
        ring = features[0]['geometry']['coordinates'][0]
        assert ring[0] == ring[-1]
        assert ring[0][0] < -40  # longitude antes de latitude
        assert api.get('/api/clusters?day=bad').status_code == 422
        assert api.get('/api/clusters?day=2026-11-10&cursor=bad').status_code == 400
        assert api.get('/').status_code == 200


def test_fifo_stops_at_first_failure(aws):
    item = record('2026-11-10', 'AQUA', -10, -50)
    response = handler({'Records': [{'messageId': 'bad', 'body': '{}'}, event([item])['Records'][0]]}, None)
    assert response['batchItemFailures'] == [{'itemIdentifier': 'bad'}, {'itemIdentifier': 'm1'}]
    assert aws.scan(TableName='test-clusters')['Count'] == 0


def test_ten_minute_grouping_and_legacy_isolation(aws):
    first = record('2026-11-10T15:40:00Z', 'AQUA', -10, -50)
    same_window = record('2026-11-10T15:49:59Z', 'AQUA', -10, -50)
    next_window = record('2026-11-10T15:50:00Z', 'AQUA', -10, -50)
    # Marcadores e agregados da implementação diária não bloqueiam o reprocessamento.
    aws.put_item(TableName='test-clusters', Item={'pk': {'S': f"EVENT#{first['id']}"}, 'sk': {'S': 'RES#5'}})
    aws.put_item(TableName='test-clusters', Item={'pk': {'S': 'DAY#2026-11-10'}, 'sk': {'S': 'RES#5#H3#legacy'}, 'count': {'N': '99'}})
    assert handler(event([first, same_window, next_window]), None)['batchItemFailures'] == []
    assert handler(event([first, same_window, next_window]), None)['batchItemFailures'] == []
    with TestClient(app) as api:
        features = api.get('/api/clusters?day=2026-11-10').json()['features']
        assert {f['properties']['date']: f['properties']['count'] for f in features} == {
            '2026-11-10T15:40:00Z': 2, '2026-11-10T15:50:00Z': 1}
        page = api.get('/api/clusters?day=2026-11-10&window=15:40').json()
        assert len(page['features']) == 1
        assert page['features'][0]['properties']['window_end'] == '2026-11-10T15:50:00Z'
        assert api.get('/api/clusters?day=2026-11-10&window=15:41').status_code == 422


def test_function_url_with_real_query(aws):
    from backend.handler import handler as api_handler
    item = record('2026-11-10T15:41:00Z', 'AQUA', -10, -50)
    assert handler(event([item]), None)['batchItemFailures'] == []
    result = api_handler({'version': '2.0', 'rawPath': '/api/clusters',
                          'requestContext': {'http': {'method': 'GET'}},
                          'queryStringParameters': {'day': '2026-11-10', 'window': '15:40'}}, None)
    assert result['statusCode'] == 200
    assert json.loads(result['body'])['features'][0]['properties']['count'] == 1


def test_gsi_total_and_exact_recent_window(aws):
    from backend.query import query_observations
    items = [record('2000-01-01T23:35:00Z', 'AQUA', -10, -50),
             record('2000-01-02T00:04:00Z', 'AQUA', -10, -50),
             record('2000-01-02T00:09:00Z', 'AQUA', -10, -50)]
    assert handler(event(items), None)['batchItemFailures'] == []
    total = query_observations()
    assert sum(f['properties']['count'] for f in total['features']) == 3
    recent = query_observations('recent', minutes=10, as_of='2000-01-02T00:15:00Z')
    # 00:04 e 00:09 estão no mesmo bucket legado; só 00:09 cabe nos últimos 10 min.
    assert sum(f['properties']['count'] for f in recent['features']) == 1
    crossing = query_observations('recent', minutes=60, as_of='2000-01-02T00:15:00Z')
    assert sum(f['properties']['count'] for f in crossing['features']) == 3
