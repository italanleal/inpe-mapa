#!/usr/bin/env bash
# Executar na máquina local ou no AWS CloudShell; destino: Python 3.12/x86_64.
set -euo pipefail
project_dir="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd)"
build_dir="$(mktemp -d)"
trap 'rm -rf -- "$build_dir"' EXIT
python3 -m pip install \
  --platform manylinux_2_28_x86_64 \
  --platform manylinux2014_x86_64 \
  --implementation cp --python-version 3.12 --abi cp312 \
  --only-binary=:all: --no-compile \
  --target "$build_dir" -r "$project_dir/consumer/requirements.txt"
cp "$project_dir/consumer/handler.py" "$build_dir/handler.py"
mkdir -p "$build_dir/common"
cp "$project_dir/common/__init__.py" "$project_dir/common/records.py" "$build_dir/common/"
mkdir -p "$project_dir/dist"
python3 - "$build_dir" "$project_dir/dist/inpe-consumer.zip" <<'PY'
from pathlib import Path
import sys
from zipfile import ZipFile, ZIP_DEFLATED
source, output = map(Path, sys.argv[1:])
with ZipFile(output, 'w', ZIP_DEFLATED) as archive:
    for path in sorted(source.rglob('*')):
        if path.is_file() and '__pycache__' not in path.parts and path.suffix != '.pyc':
            archive.write(path, path.relative_to(source))
with ZipFile(output) as archive:
    names = archive.namelist()
    assert 'handler.py' in names and 'common/records.py' in names
    assert any(n.startswith('h3/') and n.endswith('.so') and 'cpython-312' in n for n in names), 'Binário H3 CPython 3.12 ausente'
    assert 'boto3/__init__.py' in names
    assert archive.testzip() is None
print(f'Pacote gerado: {output}')
print('Runtime: Python 3.12 | Arquitetura: x86_64 | Handler: handler.handler')
PY
