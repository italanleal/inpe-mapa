#!/usr/bin/env python3
"""Empacota o frontend para deploy no AWS Elastic Beanstalk (plataforma Python, sem dependências)."""
import argparse
import json
from pathlib import Path
from urllib.parse import urlsplit
from zipfile import ZipFile, ZIP_DEFLATED

APPLICATION_PY = '''"""Servidor WSGI mínimo para servir o mapa estático no AWS Elastic Beanstalk."""
import mimetypes
from pathlib import Path

STATIC_DIR = Path(__file__).resolve().parent / 'static'
ALLOWED_FILES = {'index.html', 'style.css', 'data.js', 'app.js', 'config.js'}


def application(environ, start_response):
    path = environ.get('PATH_INFO', '/') or '/'
    if path == '/health':
        start_response('200 OK', [('Content-Type', 'text/plain; charset=utf-8')])
        return [b'ok']
    name = 'index.html' if path == '/' else path.lstrip('/')
    if name not in ALLOWED_FILES:
        start_response('404 Not Found', [('Content-Type', 'text/plain; charset=utf-8')])
        return [b'Not Found']
    file_path = STATIC_DIR / name
    if not file_path.is_file():
        start_response('404 Not Found', [('Content-Type', 'text/plain; charset=utf-8')])
        return [b'Not Found']
    content_type, _ = mimetypes.guess_type(name)
    if not content_type:
        content_type = 'application/octet-stream'
    if content_type.startswith('text/') or content_type.endswith('javascript'):
        content_type += '; charset=utf-8'
    body = file_path.read_bytes()
    start_response('200 OK', [
        ('Content-Type', content_type),
        ('Content-Length', str(len(body))),
        ('Cache-Control', 'no-cache'),
    ])
    return [body]
'''


def build(api_url, output):
    parsed = urlsplit(api_url)
    if (parsed.scheme != 'https' or not parsed.hostname or parsed.username
            or parsed.password or parsed.query or parsed.fragment or parsed.path not in ('', '/')):
        raise ValueError('Informe a origem HTTPS da Function URL, sem caminho, credenciais ou query string')
    root = Path(__file__).resolve().parents[1]
    output = Path(output)
    output.parent.mkdir(parents=True, exist_ok=True)
    config = {'apiBaseUrl': api_url.rstrip('/'), 'refreshIntervalMs': 5000}
    with ZipFile(output, 'w', ZIP_DEFLATED) as archive:
        archive.writestr('application.py', APPLICATION_PY)
        for name in ('index.html', 'style.css', 'data.js', 'app.js'):
            archive.write(root / 'frontend' / name, f'static/{name}')
        archive.writestr('static/config.js', 'window.INPE_CONFIG = ' + json.dumps(config) + ';\n')
    return output


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--api-url', required=True, help='URL pública HTTPS da Lambda de consulta')
    parser.add_argument('--output', default='dist/inpe-mapa-eb.zip', help='Caminho do ZIP de saída')
    args = parser.parse_args()
    try:
        print(f'Pacote Elastic Beanstalk pronto: {build(args.api_url, args.output)}')
    except ValueError as error:
        parser.error(str(error))
