"""Contrato comum entre o produtor e o consumidor; datas sempre em UTC."""
import hashlib
import math
import unicodedata
from datetime import datetime, timedelta, timezone


def key(value):
    return ''.join(c for c in unicodedata.normalize('NFKD', str(value).lower())
                   if c.isalnum() and not unicodedata.combining(c))


def timestamp(value):
    dt = datetime.fromisoformat(str(value).strip().replace('Z', '+00:00'))
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)  # INPE: data/hora GMT
    return dt.astimezone(timezone.utc).isoformat(timespec='microseconds').replace('+00:00', 'Z')


def record(date, satellite, latitude, longitude):
    date = timestamp(date)
    satellite = str(satellite).strip().upper()
    latitude, longitude = float(latitude), float(longitude)
    if not satellite or len(satellite) > 100:
        raise ValueError('Satélite vazio ou maior que 100 caracteres')
    if not math.isfinite(latitude) or not -90 <= latitude <= 90:
        raise ValueError('Latitude inválida')
    if not math.isfinite(longitude) or not -180 <= longitude <= 180:
        raise ValueError('Longitude inválida')
    identity = f'{date}|{satellite}|{latitude:.8f}|{longitude:.8f}'
    return dict(id=hashlib.sha256(identity.encode()).hexdigest(), date=date,
                satellite=satellite, latitude=latitude, longitude=longitude)


def from_csv(row, brazil_only=True):
    row = {key(k): v for k, v in row.items() if k is not None}
    def pick(*names):
        for name in names:
            if row.get(name) not in (None, ''):
                return row[name]
        raise ValueError(f'Coluna obrigatória ausente: {names}')
    # CSVs de 10 min podem conter apenas lat, lon, satelite e data.
    has_country = 'pais' in row or 'country' in row
    if brazil_only and has_country and key(pick('pais', 'country')) not in ('brasil', 'brazil', 'bra', 'br'):
        return None
    return record(pick('datahoragmt', 'datahora', 'data', 'datetime', 'timestamp'),
                  pick('satelite', 'satellite'), pick('lat', 'latitude'),
                  pick('lon', 'long', 'longitude'))


AGGREGATION = 'WINDOW10M'


def ten_minute_window(value):
    """Janela UTC [início, fim), determinada pelo horário da observação."""
    dt = datetime.fromisoformat(timestamp(value).replace('Z', '+00:00'))
    start = dt.replace(minute=(dt.minute // 10) * 10, second=0, microsecond=0)
    end = start + timedelta(minutes=10)
    return tuple(t.isoformat(timespec='seconds').replace('+00:00', 'Z') for t in (start, end))
