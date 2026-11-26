/* O navegador usa somente a API HTTP; nenhuma credencial AWS chega ao cliente. */
'use strict';
const config = window.INPE_CONFIG || {};
const apiBaseUrl = (config.apiBaseUrl || '').replace(/\/+$/, '');
const refreshIntervalMs = Math.max(1000, Number(config.refreshIntervalMs) || 5000);
const BRAZIL_VIEW = {center: {lat: -14.235, lon: -51.925}, zoom: 3.4};
const MAP_STYLES = new Set(['carto-positron', 'carto-voyager', 'carto-darkmatter', 'white-bg']);
const $ = id => document.getElementById(id);
const state = {features: [], busy: false, lastSignature: '', viewRevision: 1};

const PLOTLY_CONFIG = {
  responsive: true,
  scrollZoom: true,
  displaylogo: false,
  displayModeBar: true,
  modeBarButtonsToRemove: ['lasso2d', 'select2d'],
};

function buildTrace(cells) {
  const scale = $('colorscale').value || 'Viridis';
  const geojson = {
    type: 'FeatureCollection',
    features: cells.map(cell => ({
      type: 'Feature',
      id: cell.h3,
      geometry: cell.feature.geometry,
      properties: {h3: cell.h3},
    })),
  };
  return {
    type: 'choroplethmap',
    geojson,
    locations: cells.map(c => c.h3),
    z: cells.map(c => c.count),
    customdata: cells.map(c => [c.h3, c.satellites.join(', ')]),
    colorscale: scale,
    reversescale: scale === 'YlOrRd',
    zmin: 1,
    marker: {
      opacity: 0.6,
      line: {width: 0.4, color: 'rgba(255,255,255,0.45)'},
    },
    colorbar: {
      title: {text: 'Focos', font: {size: 12, color: '#2a3f5f'}},
      thickness: 18,
      len: 0.65,
      x: 0.99,
      xanchor: 'right',
      y: 0.5,
      bgcolor: 'rgba(255,255,255,0.85)',
      borderwidth: 0,
      tickfont: {size: 11, color: '#2a3f5f'},
    },
    hovertemplate:
      '<b>%{z:,} focos de calor</b><br>' +
      'Satélites: %{customdata[1]}<br>' +
      'H3: %{customdata[0]}<extra></extra>',
  };
}

function buildLayout() {
  const styleKey = $('map-style').value;
  return {
    margin: {t: 0, r: 0, b: 0, l: 0},
    paper_bgcolor: '#ffffff',
    uirevision: `inpe-h3-${state.viewRevision}`,
    hoverlabel: {
      font: {family: 'system-ui, sans-serif', size: 12},
    },
    map: {
      style: MAP_STYLES.has(styleKey) ? styleKey : 'carto-positron',
      center: BRAZIL_VIEW.center,
      zoom: BRAZIL_VIEW.zoom,
    },
  };
}

function draw(force = false) {
  const satellite = $('satellite').value;
  const styleKey = $('map-style').value;
  const scale = $('colorscale').value;
  const cells = window.INPE_DATA.aggregateCells(state.features, satellite);
  const total = cells.reduce((sum, d) => sum + d.count, 0);
  const uniqueSats = new Set(cells.flatMap(cell => cell.satellites)).size;
  const signature = `${styleKey}|${scale}|${satellite}|${state.viewRevision}|${cells.length}|${total}|${cells.map(c => `${c.h3}:${c.count}`).join(',')}`;

  $('total').textContent = total.toLocaleString('pt-BR');
  $('cells').textContent = cells.length.toLocaleString('pt-BR');
  $('satellites').textContent = uniqueSats.toLocaleString('pt-BR');

  if (!force && signature === state.lastSignature) return;
  state.lastSignature = signature;

  Plotly.react('map', [buildTrace(cells)], buildLayout(), PLOTLY_CONFIG);
}

async function getJSON(url) {
  const response = await fetch(url, {signal: AbortSignal.timeout(20000), cache: 'no-store'});
  if (!response.ok) throw new Error(`HTTP ${response.status}`);
  return response.json();
}

async function refresh() {
  if (state.busy) return;
  state.busy = true;
  $('status').textContent = 'Consultando DynamoDB…';
  try {
    let cursor = null;
    let queriedAt = null;
    const features = [];
    do {
      const params = new URLSearchParams({limit: '1000'});
      if (cursor) params.set('cursor', cursor);
      const page = await getJSON(`${apiBaseUrl}/api/clusters?${params}`);
      if (!Array.isArray(page.features)) {
        throw new Error('Resposta inválida da API de consulta');
      }
      queriedAt = page.queried_at || queriedAt;
      features.push(...page.features);
      cursor = page.next_cursor;
    } while (cursor);
    state.features = features;
    const selected = $('satellite').value;
    const satellites = [...new Set(features.map(f => f.properties.satellite))].sort();
    $('satellite').replaceChildren(new Option('Todos os satélites', ''), ...satellites.map(s => new Option(s, s)));
    $('satellite').value = satellites.includes(selected) ? selected : '';
    draw();
    $('status').textContent = features.length
      ? `● Dados atualizados · a cada ${refreshIntervalMs / 1000} s`
      : 'Sem focos registrados.';
    $('updated').textContent = queriedAt
      ? `Última consulta: ${new Date(queriedAt).toLocaleTimeString('pt-BR')}`
      : 'Atualizado';
  } catch (error) {
    $('status').textContent = `Falha ao atualizar (${error.message}). Nova tentativa automática…`;
  } finally {
    state.busy = false;
  }
}

async function clearDatabase() {
  if (state.busy) return;
  if (!window.confirm('Apagar TODOS os registros de focos no DynamoDB? Esta ação não pode ser desfeita.')) return;
  state.busy = true;
  $('clear').disabled = true;
  $('status').textContent = 'Limpando banco de dados…';
  try {
    const response = await fetch(`${apiBaseUrl}/api/clusters`, {
      method: 'DELETE',
      signal: AbortSignal.timeout(25000),
      cache: 'no-store',
    });
    if (!response.ok) throw new Error(`HTTP ${response.status}`);
    const result = await response.json();
    state.features = [];
    $('satellite').replaceChildren(new Option('Todos os satélites', ''));
    draw(true);
    const deleted = Number(result.deleted || 0).toLocaleString('pt-BR');
    $('status').textContent = `Banco limpo (${deleted} registros removidos).`;
    $('updated').textContent = 'Banco vazio';
  } catch (error) {
    $('status').textContent = `Falha ao limpar banco (${error.message}).`;
  } finally {
    state.busy = false;
    $('clear').disabled = false;
  }
}

$('satellite').onchange = () => draw(true);
$('map-style').onchange = () => draw(true);
$('colorscale').onchange = () => draw(true);
$('refresh').onclick = refresh;
$('clear').onclick = clearDatabase;
$('center-br').onclick = () => {
  state.viewRevision += 1;
  draw(true);
};

async function poll() {
  await refresh();
  setTimeout(poll, refreshIntervalMs);
}

draw(true);
poll();
