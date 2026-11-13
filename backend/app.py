"""Servidor opcional para desenvolvimento local; usa a mesma consulta da Lambda."""
import logging
from datetime import date, datetime, timezone
from pathlib import Path

from botocore.exceptions import BotoCoreError, ClientError
from fastapi import FastAPI, HTTPException, Query
from fastapi.staticfiles import StaticFiles
from backend.query import query_clusters, query_observations

app = FastAPI(title='Focos INPE • H3', version='2.0.0')
LOG = logging.getLogger(__name__)


@app.get('/api/health')
def health():
    return {'status': 'ok', 'time': datetime.now(timezone.utc).isoformat()}


@app.get('/api/clusters')
def clusters(day: date | None = None, satellite: str | None = Query(None, max_length=100),
             window: str | None = Query(None, pattern=r'^(?:[01][0-9]|2[0-3]):[0-5]0$'),
             cursor: str | None = Query(None, max_length=4096),
             limit: int = Query(500, ge=1, le=1000),
             view: str | None = None, minutes: int | None = None, as_of: str | None = None):
    try:
        if day is not None and view is None:
            return query_clusters(day.isoformat(), satellite, window, cursor, limit)
        return query_observations(view or 'all', minutes, satellite, cursor, limit, as_of)
    except ValueError as error:
        raise HTTPException(400, str(error)) from error
    except (ClientError, BotoCoreError):
        LOG.exception('Erro consultando DynamoDB')
        raise HTTPException(503, 'DynamoDB indisponível; confira região, tabela e credenciais')


app.mount('/', StaticFiles(directory=Path(__file__).resolve().parents[1] / 'frontend', html=True), name='frontend')
