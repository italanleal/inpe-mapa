"""Validação e paginação da consulta com SDK/geometria substituídos por doubles."""
import base64
import importlib.util
import json
import sys
from types import ModuleType
from unittest.mock import Mock

import pytest


@pytest.fixture
def query_module(monkeypatch):
    class Key:
        def __init__(self, name): self.name = name
        def eq(self, value): return self
        def begins_with(self, value): return self
        def between(self, start, end): self.bounds = (start, end); return self
        def __and__(self, other):
            if hasattr(other, 'bounds'): self.bounds = other.bounds
            return self
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
    monkeypatch.setenv('H3_RESOLUTION', '5')
    spec = importlib.util.spec_from_file_location('query_under_test', 'backend/query.py')
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module, table


@pytest.mark.parametrize('params', [
    {'day': None}, {'day': '2026-02-30'}, {'day': '20261113'},
    {'day': '2026-11-13', 'window': '15:41'}, {'day': '2026-11-13', 'window': '24:00'},
    {'day': '2026-11-13', 'limit': 1001}, {'day': '2026-11-13', 'limit': 0},
    {'day': '2026-11-13', 'cursor': 'bad'},
])
def test_validation_before_database_call(query_module, params):
    module, table = query_module
    with pytest.raises(ValueError): module.query_clusters(**params)
    table.query.assert_not_called()


def test_geojson_cursor_and_satellite_filter(query_module):
    module, table = query_module
    key = {'pk': 'DAY#2026-11-13', 'sk': 'RES#5#WINDOW10M#2026-11-13T15:40:00Z#H3#abc#SAT#AQUA'}
    item = {'date': '2026-11-13T15:40:00Z', 'window_end': '2026-11-13T15:50:00Z',
            'satellite': 'AQUA', 'h3': 'abc', 'count': 2, 'resolution': 5, 'updated_at': 'now'}
    table.query.return_value = {'Items': [item], 'LastEvaluatedKey': key}
    result = module.query_clusters('2026-11-13', window='15:40')
    ring = result['features'][0]['geometry']['coordinates'][0]
    assert ring[0] == [-50, -10] and ring[-1] == ring[0]
    assert result['features'][0]['properties']['count'] == 2
    assert json.loads(base64.urlsafe_b64decode(result['next_cursor'])) == key
    module.query_clusters('2026-11-13', window='15:40', cursor=result['next_cursor'])
    assert table.query.call_args.kwargs['ExclusiveStartKey'] == key
    assert table.query.call_args.kwargs['ConsistentRead'] is True
    assert module.query_clusters('2026-11-13', satellite='TERRA')['features'] == []
    with pytest.raises(ValueError): module.query_clusters('2026-11-14', cursor=result['next_cursor'])
    with pytest.raises(ValueError): module.query_clusters('2026-11-13', window='15:50', cursor=result['next_cursor'])


def test_observations_all_groups_same_hash_across_hours(query_module):
    module, table = query_module
    table.query.return_value = {'Items': [
        {'h3': 'abc', 'satellite': 'AQUA', 'date': '2026-11-13T01:00:00.000000Z'},
        {'h3': 'abc', 'satellite': 'AQUA', 'date': '2026-11-13T02:00:00.000000Z'},
        {'h3': 'abc', 'satellite': 'TERRA', 'date': '2026-11-12T02:00:00.000000Z'}]}
    result = module.query_observations()
    assert sum(f['properties']['count'] for f in result['features']) == 3
    assert len({f['properties']['h3'] for f in result['features']}) == 1
    assert result['features'][0]['properties']['count'] == 2
    request = table.query.call_args.kwargs
    assert request['IndexName'] == 'ObservationsByTime'
    assert 'ConsistentRead' not in request


def test_recent_cutoff_crosses_midnight_and_cursor_keeps_reference(query_module):
    module, table = query_module
    key = {'pk': 'EVENT#abc', 'sk': 'RES#5#WINDOW10M', 'date': '2026-11-12T23:50:00.000000Z'}
    table.query.return_value = {'Items': [], 'LastEvaluatedKey': key}
    result = module.query_observations('recent', minutes=60, as_of='2026-11-13T00:30:00Z')
    assert result['cutoff'] == '2026-11-12T23:30:00.000000Z'
    assert table.query.call_args.kwargs['KeyConditionExpression'].bounds == ('2026-11-12T23:30:00.000000Z', '2026-11-13T00:30:00.000000Z')
    assert result['reference_time'] == '2026-11-13T00:30:00.000000Z'
    module.query_observations('recent', minutes=60, as_of=result['reference_time'], cursor=result['next_cursor'])
    assert table.query.call_args.kwargs['ExclusiveStartKey'] == key
    with pytest.raises(ValueError):
        module.query_observations('recent', minutes=30, as_of=result['reference_time'], cursor=result['next_cursor'])


@pytest.mark.parametrize('kwargs', [
    {'view': 'bad'}, {'view': 'recent'}, {'view': 'recent', 'minutes': 0},
    {'view': 'recent', 'minutes': 1.5}, {'view': 'all', 'minutes': 60},
    {'as_of': 'bad'}, {'as_of': '2026-11-13T01:00:00'}, {'limit': 0}, {'cursor': 'bad'}])
def test_observation_validation(query_module, kwargs):
    module, table = query_module
    with pytest.raises(ValueError): module.query_observations(**kwargs)
    table.query.assert_not_called()
