import importlib.util
import json
from pathlib import Path
from zipfile import ZipFile

import pytest

spec = importlib.util.spec_from_file_location('build_frontend', 'scripts/build-frontend.py')
module = importlib.util.module_from_spec(spec)
spec.loader.exec_module(module)


def test_static_bundle_has_root_index_and_public_config(tmp_path):
    output = module.build('https://example.lambda-url.sa-east-1.on.aws/', tmp_path / 'site.zip')
    with ZipFile(output) as archive:
        assert set(archive.namelist()) == {'index.html', 'config.js', 'data.js', 'app.js', 'style.css'}
        config = archive.read('config.js').decode()
        assert 'https://example.lambda-url.sa-east-1.on.aws' in config
        assert archive.read('index.html').decode().index('./config.js') < archive.read('index.html').decode().index('./app.js')
        assert 'apiBaseUrl' in archive.read('app.js').decode()
        assert archive.testzip() is None


@pytest.mark.parametrize('url', ['http://example.com', 'https://user:secret@example.com', 'https://example.com/path', 'https://example.com?key=secret', 'https://example.com#x'])
def test_static_bundle_rejects_invalid_url(url, tmp_path):
    with pytest.raises(ValueError):
        module.build(url, tmp_path / 'site.zip')


def test_h3_aggregation_in_javascript():
    import shutil
    import subprocess
    if not shutil.which('node'):
        pytest.skip('Node não instalado')
    subprocess.run(['node', 'tests/test_map_data.js'], check=True, capture_output=True, text=True)
