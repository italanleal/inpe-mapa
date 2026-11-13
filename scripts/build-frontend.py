"""Empacota o frontend estático com a URL pública da Lambda, sem dependências."""
import argparse
import json
from pathlib import Path
from urllib.parse import urlsplit
from zipfile import ZipFile, ZIP_DEFLATED


def build(api_url, output):
    parsed = urlsplit(api_url)
    if parsed.scheme != 'https' or not parsed.hostname or parsed.username or parsed.password or parsed.query or parsed.fragment or parsed.path not in ('', '/'):
        raise ValueError('Informe a origem HTTPS da Function URL, sem caminho, credenciais ou query string')
    root = Path(__file__).resolve().parents[1]
    output = Path(output)
    output.parent.mkdir(parents=True, exist_ok=True)
    config = {'apiBaseUrl': api_url.rstrip('/'), 'refreshIntervalMs': 5000}
    with ZipFile(output, 'w', ZIP_DEFLATED) as archive:
        for name in ('index.html', 'style.css', 'data.js', 'app.js'):
            archive.write(root / 'frontend' / name, name)
        archive.writestr('config.js', 'window.INPE_CONFIG = ' + json.dumps(config) + ';\n')
    return output


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--api-url', required=True)
    parser.add_argument('--output', default='dist/inpe-mapa.zip')
    args = parser.parse_args()
    try:
        print(f'Website pronto para upload: {build(args.api_url, args.output)}')
    except ValueError as error:
        parser.error(str(error))
