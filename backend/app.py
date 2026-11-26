"""Servidor opcional para desenvolvimento local; usa a mesma consulta da Lambda."""
import logging
from datetime import datetime, timezone
from pathlib import Path

from botocore.exceptions import BotoCoreError, ClientError
from fastapi import FastAPI, HTTPException, Query
from fastapi.staticfiles import StaticFiles
from backend.query import clear_clusters, query_clusters

app = FastAPI(title='Focos INPE • H3', version='3.0.0')
LOG = logging.getLogger(__name__)


@app.get('/api/health')
def health():
    return {'status': 'ok', 'time': datetime.now(timezone.utc).isoformat()}


@app.get('/api/clusters')
def clusters(
    satellite: str | None = Query(None, max_length=100),
    cursor: str | None = Query(None, max_length=4096),
    limit: int = Query(500, ge=1, le=1000),
):
    try:
        return query_clusters(satellite=satellite, cursor=cursor, limit=limit)
    except ValueError as error:
        raise HTTPException(400, str(error)) from error
    except (ClientError, BotoCoreError):
        LOG.exception('Erro consultando DynamoDB')
        raise HTTPException(503, 'DynamoDB indisponível; confira região, tabela e credenciais')


@app.delete('/api/clusters')
def delete_clusters():
    try:
        return clear_clusters()
    except (ClientError, BotoCoreError):
        LOG.exception('Erro limpando DynamoDB')
        raise HTTPException(503, 'DynamoDB indisponível; confira região, tabela e credenciais')


app.mount('/', StaticFiles(directory=Path(__file__).resolve().parents[1] / 'frontend', html=True), name='frontend')
