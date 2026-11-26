"""Carregamento simples de arquivo .env sem dependências externas."""
import os
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]


def load_dotenv(path=None):
    """Carrega variáveis de um arquivo .env sem sobrescrever variáveis já definidas no ambiente."""
    env_path = Path(path) if path is not None else PROJECT_ROOT / '.env'
    if not env_path.is_file():
        return False
    for raw_line in env_path.read_text(encoding='utf-8').splitlines():
        line = raw_line.strip()
        if not line or line.startswith('#'):
            continue
        if line.startswith('export '):
            line = line[7:].strip()
        if '=' not in line:
            continue
        name, value = line.split('=', 1)
        name = name.strip()
        value = value.strip()
        if not name:
            continue
        if len(value) >= 2 and value[0] == value[-1] and value[0] in ("'", '"'):
            value = value[1:-1]
        os.environ.setdefault(name, value)
    return True
