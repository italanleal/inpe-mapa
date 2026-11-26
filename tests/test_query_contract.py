"""Validação e paginação da consulta e do consumidor com SDK/geometria substituídos por doubles."""
import base64
import importlib.util
import json
import sys
from types import ModuleType
from unittest.mock import Mock

import pytest

from common.records import record


@pytest.fixture
def query_module(monkeypatch):
    class Key:
        def __init__(self, name): self.name = name
        def eq(self, value): self.value = value; return self
    sdk = ModuleType('boto3')
    table = Mock()
    sdk.resource = Mock()
    sdk.resource.return_value.Table.return_value = table
    conditions = ModuleType('boto3.dynamodb.conditions')
    conditions.Key = Key
    geometry = ModuleType('h3')
    geometry.cell_to_boundary = Mock(return_value=[(-10, -50), (-11, -50), (-10, -51)])
    monkeypatch.setitem(sys.modules, 'boto3', sdk)
    monkeypatch.setitem(sys.modules, 'boto3.dynamodb.conditions', conditions)
    monkeypatch.setitem(sys.modules, 'h3', geometry)
    monkeypatch.setenv('TABLE_NAME', 'clusters')
    spec = importlib.util.spec_from_file_location('query_under_test', 'backend/query.py')
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module, table


@pytest.mark.parametrize('params', [
    {'limit': 1001}, {'limit': 0}, {'limit': True},
    {'satellite': 'A' * 101}, {'cursor': 'bad'},
])
def test_validation_before_database_call(query_module, params):
    module, table = query_module
    with pytest.raises(ValueError):
        module.query_clusters(**params)
    table.query.assert_not_called()


def test_geojson_cursor_and_satellite_filter(query_module):
    module, table = query_module
    key = {'pk': 'CLUSTER', 'sk': 'H3#abc#SAT#AQUA'}
    item = {'pk': 'CLUSTER', 'sk': 'H3#abc#SAT#AQUA', 'h3': 'abc', 'satellite': 'AQUA', 'count': 4}
    table.query.return_value = {'Items': [item], 'LastEvaluatedKey': key}
    result = module.query_clusters()
    ring = result['features'][0]['geometry']['coordinates'][0]
    assert ring[0] == [-50, -10] and ring[-1] == ring[0]
    assert result['features'][0]['properties'] == {'h3': 'abc', 'satellite': 'AQUA', 'count': 4}
    assert json.loads(base64.urlsafe_b64decode(result['next_cursor'])) == key
    module.query_clusters(cursor=result['next_cursor'])
    assert table.query.call_args.kwargs['ExclusiveStartKey'] == key
    assert table.query.call_args.kwargs['ConsistentRead'] is True
    assert module.query_clusters(satellite='TERRA')['features'] == []
    bad_cursor = base64.urlsafe_b64encode(json.dumps({'pk': 'DAY#2026-11-13', 'sk': 'H3#abc#SAT#AQUA'}).encode()).decode()
    with pytest.raises(ValueError):
        module.query_clusters(cursor=bad_cursor)


def test_consumer_persists_only_h3_satellite_and_count(monkeypatch):
    sdk = ModuleType('boto3')
    client = Mock()
    sdk.client = Mock(return_value=client)
    types_mod = ModuleType('boto3.dynamodb.types')

    class DummySerializer:
        def serialize(self, v):
            return {'N': str(v)} if isinstance(v, int) else {'S': str(v)}

    types_mod.TypeSerializer = DummySerializer
    geometry = ModuleType('h3')
    geometry.latlng_to_cell = Mock(return_value='8580104ffffffff')
    monkeypatch.setitem(sys.modules, 'boto3', sdk)
    monkeypatch.setitem(sys.modules, 'boto3.dynamodb', ModuleType('boto3.dynamodb'))
    monkeypatch.setitem(sys.modules, 'boto3.dynamodb.types', types_mod)
    monkeypatch.setitem(sys.modules, 'h3', geometry)
    monkeypatch.setenv('TABLE_NAME', 'clusters')
    monkeypatch.setenv('H3_RESOLUTION', '5')

    spec = importlib.util.spec_from_file_location('consumer_under_test', 'consumer/handler.py')
    consumer = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(consumer)

    item = record('2026-11-22T00:00:00Z', 'GOES-19', -1.1653, -53.4756)
    event = {'Records': [{'messageId': 'm1', 'body': json.dumps({'schema_version': 1, 'records': [item]})}]}
    assert consumer.handler(event, None) == {'batchItemFailures': []}

    client.update_item.assert_called_once()
    kwargs = client.update_item.call_args.kwargs
    assert kwargs['Key'] == {'pk': {'S': 'CLUSTER'}, 'sk': {'S': 'H3#8580104ffffffff#SAT#GOES-19'}}
    assert kwargs['UpdateExpression'] == 'SET h3 = :h, satellite = :s ADD #n :one'
    assert kwargs['ExpressionAttributeNames'] == {'#n': 'count'}
    assert kwargs['ExpressionAttributeValues'] == {
        ':h': {'S': '8580104ffffffff'},
        ':s': {'S': 'GOES-19'},
        ':one': {'N': '1'},
    }


def test_clear_clusters_scans_all_pages_and_deletes_keys(query_module):
    module, table = query_module
    batch = Mock()
    batch_cm = Mock()
    batch_cm.__enter__ = Mock(return_value=batch)
    batch_cm.__exit__ = Mock(return_value=False)
    table.batch_writer.return_value = batch_cm
    table.scan.side_effect = [
        {'Items': [{'pk': 'CLUSTER', 'sk': 'H3#a#SAT#AQUA'}], 'LastEvaluatedKey': {'pk': 'CLUSTER', 'sk': 'H3#a#SAT#AQUA'}},
        {'Items': [{'pk': 'DAY#2026-11-10', 'sk': 'OLD'}]},
    ]
    result = module.clear_clusters()
    assert result['deleted'] == 2
    assert batch.delete_item.call_count == 2
    batch.delete_item.assert_any_call(Key={'pk': 'CLUSTER', 'sk': 'H3#a#SAT#AQUA'})
    batch.delete_item.assert_any_call(Key={'pk': 'DAY#2026-11-10', 'sk': 'OLD'})
