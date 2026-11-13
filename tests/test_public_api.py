"""Contrato HTTP da Function URL; consulta substituída por um stub explícito."""
import importlib.util
import json
import sys
from types import ModuleType
from unittest.mock import Mock

import pytest


@pytest.fixture
def api(monkeypatch):
    service = ModuleType('backend.query')
    service.query_observations = Mock(return_value={'type': 'FeatureCollection', 'features': [], 'reference_time': '2026-11-13T15:00:00.000000Z'})
    service.query_clusters = Mock(return_value={'type': 'FeatureCollection', 'features': [], 'next_cursor': None})
    errors = ModuleType('botocore.exceptions')
    errors.BotoCoreError = type('BotoCoreError', (Exception,), {})
    errors.ClientError = type('ClientError', (Exception,), {})
    monkeypatch.setitem(sys.modules, 'backend.query', service)
    monkeypatch.setitem(sys.modules, 'botocore.exceptions', errors)
    spec = importlib.util.spec_from_file_location('public_api_under_test', 'backend/handler.py')
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module, service.query_clusters, errors


def event(path='/api/clusters', method='GET', params=None):
    return {'version': '2.0', 'rawPath': path, 'requestContext': {'http': {'method': method}},
            'queryStringParameters': params}


def test_function_url_success_and_pagination_forwarded(api):
    module, query, _ = api
    result = module.handler(event(params={'day': '2026-11-13', 'window': '15:40', 'cursor': 'abc', 'limit': '10'}), None)
    assert result['statusCode'] == 200
    assert json.loads(result['body'])['type'] == 'FeatureCollection'
    query.assert_called_once_with(day='2026-11-13', window='15:40', satellite=None, cursor='abc', limit=10)
    assert result['headers']['Cache-Control'] == 'no-store'
    assert not any(h.lower().startswith('access-control') for h in result['headers'])


@pytest.mark.parametrize('path', ['/', '/api/health'])
def test_health_does_not_query_database(api, path):
    module, query, _ = api
    assert module.handler(event(path), None)['statusCode'] == 200
    query.assert_not_called()


def test_methods_and_routes(api):
    module, query, _ = api
    assert module.handler(event(method='POST'), None)['statusCode'] == 405
    assert module.handler(event('/unknown'), None)['statusCode'] == 404
    query.assert_not_called()


def test_validation_returns_400(api):
    module, query, _ = api
    assert module.handler(event(params={'limit': 'bad'}), None)['statusCode'] == 400
    query.assert_not_called()
    query.side_effect = ValueError('day inválido')
    assert module.handler(event(params={'day': 'bad'}), None)['statusCode'] == 400


def test_errors_do_not_expose_internal_details(api):
    module, query, errors = api
    query.side_effect = errors.ClientError('secret configuration')
    result = module.handler(event(params={'day': '2026-11-13'}), None)
    assert result['statusCode'] == 503
    assert 'secret' not in result['body']
    query.side_effect = RuntimeError('internal path')
    result = module.handler(event(params={'day': '2026-11-13'}), None)
    assert result['statusCode'] == 500
    assert 'internal path' not in result['body']


def test_recent_route_forwards_minutes_and_reference(api):
    module, _, _ = api
    service = sys.modules['backend.query'].query_observations
    result = module.handler(event(params={'view': 'recent', 'minutes': '7', 'as_of': '2026-11-13T15:00:00Z'}), None)
    assert result['statusCode'] == 200
    service.assert_called_once_with(view='recent', minutes=7, satellite=None, cursor=None, limit=500, as_of='2026-11-13T15:00:00Z')


def test_total_is_default_and_bad_minutes_fail(api):
    module, _, _ = api
    service = sys.modules['backend.query'].query_observations
    assert module.handler(event(), None)['statusCode'] == 200
    assert service.call_args.kwargs['view'] == 'all'
    service.reset_mock()
    assert module.handler(event(params={'view': 'recent', 'minutes': 'bad'}), None)['statusCode'] == 400
    service.assert_not_called()
