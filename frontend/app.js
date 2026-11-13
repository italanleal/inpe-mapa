/* O navegador usa somente a API HTTP; nenhuma credencial AWS chega ao cliente. */
'use strict';
const config = window.INPE_CONFIG || {};
const apiBaseUrl = (config.apiBaseUrl || '').replace(/\/+$/, '');
const refreshIntervalMs = Math.max(1000, Number(config.refreshIntervalMs) || 5000);
const $ = id => document.getElementById(id);
const state = {features: [], busy: false, ready: false, generation: 0, request: null};
const svg = d3.select('svg');
const scene = d3.select('#scene');
const zoom = d3.zoom().scaleExtent([1, 12]).on('zoom', event => scene.attr('transform', event.transform));
svg.call(zoom);
$('zoom-in').onclick = () => svg.transition().call(zoom.scaleBy, 1.5);
$('zoom-out').onclick = () => svg.transition().call(zoom.scaleBy, 1 / 1.5);
$('reset').onclick = () => svg.transition().call(zoom.transform, d3.zoomIdentity);
let path;

// GeoJSON usa anéis anti-horários; D3 usa sentido horário para polígonos pequenos.
function forD3(feature) {
  const result = structuredClone(feature);
  if (d3.geoArea(result) > 2 * Math.PI && result.geometry.type === 'Polygon') {
    result.geometry.coordinates = result.geometry.coordinates.map(ring => ring.reverse());
  }
  return result;
}

function draw() {
  if (!state.ready) return;
  const satellite = $('satellite').value;
  const cells = window.INPE_DATA.aggregateCells(state.features, satellite)
    .map(cell => ({...cell, feature: forD3(cell.feature)}));
  const max = d3.max(cells, d => d.count) || 1;
  const color = d3.scaleSequentialLog(d3.interpolateYlOrRd).domain([1, Math.max(2, max)]);
  $('total').textContent = d3.sum(cells, d => d.count).toLocaleString('pt-BR');
  $('cells').textContent = cells.length.toLocaleString('pt-BR');
  $('satellites').textContent = new Set(cells.flatMap(cell => cell.satellites)).size;
  $('range').textContent = cells.length ? `1 – ${max.toLocaleString('pt-BR')}` : 'Sem detecções';
  d3.select('#hexagons').selectAll('path').data(cells, d => d.h3).join('path')
    .attr('class', 'cell').attr('d', d => path(d.feature)).attr('fill', d => color(d.count))
    .attr('tabindex', 0).attr('aria-label', d => `${d.count} focos, H3 ${d.h3}, ${d.satellites.join(', ')}`)
    .on('pointerenter focus', (_, d) => {
      $('tooltip').hidden = false;
      $('tooltip').textContent = `${d.count.toLocaleString('pt-BR')} focos de calor\nPrimeira observação: ${d.first}\nÚltima observação: ${d.last}\nSatélites: ${d.satellites.join(', ')}\nH3: ${d.h3}`;
    }).on('pointerleave blur', () => { $('tooltip').hidden = true; });
}

async function getJSON(url, signal) {
  const timeout = AbortSignal.timeout(20000);
  const response = await fetch(url, {signal: signal ? AbortSignal.any([signal, timeout]) : timeout, cache: 'no-store'});
  if (!response.ok) throw new Error(`HTTP ${response.status}`);
  return response.json();
}

function timeSelection() {
  if (!$('time-filter').checked) return {view: 'all'};
  const minutes = Number($('minutes').value);
  if (!Number.isInteger(minutes) || minutes < 1 || minutes > 525600) {
    throw new Error('Informe um número inteiro de minutos entre 1 e 525600');
  }
  return {view: 'recent', minutes: String(minutes)};
}

async function refresh() {
  if (state.busy || !state.ready) return;
  let selection;
  try { selection = timeSelection(); } catch (error) {
    $('status').textContent = error.message;
    return;
  }
  state.busy = true;
  const generation = state.generation;
  state.request = new AbortController();
  $('status').textContent = 'Consultando DynamoDB…';
  try {
    let cursor = null;
    let referenceTime = null;
    const features = [];
    do {
      const params = new URLSearchParams({...selection, limit: '1000'});
      if (cursor) params.set('cursor', cursor);
      if (referenceTime) params.set('as_of', referenceTime);
      const page = await getJSON(`${apiBaseUrl}/api/clusters?${params}`, state.request.signal);
      if (!page.reference_time || !Array.isArray(page.features)) {
        throw new Error('Atualize a Lambda de consulta para habilitar a visão total e o filtro de tempo');
      }
      referenceTime = page.reference_time;
      features.push(...page.features);
      cursor = page.next_cursor;
      if (generation !== state.generation) return;
    } while (cursor);
    state.features = features;
    const selected = $('satellite').value;
    const satellites = [...new Set(features.map(f => f.properties.satellite))].sort();
    $('satellite').replaceChildren(new Option('Todos os satélites', ''), ...satellites.map(s => new Option(s, s)));
    $('satellite').value = satellites.includes(selected) ? selected : '';
    draw();
    $('status').textContent = features.length ? `● Dados atualizados · atualização a cada ${refreshIntervalMs / 1000} s` : 'Sem focos no período selecionado.';
    $('period').textContent = selection.view === 'all' ? 'Todo o histórico registrado' : `Últimos ${selection.minutes} minutos · horário da observação`;
    $('updated').textContent = `Consulta: ${new Date(referenceTime).toLocaleTimeString('pt-BR')}`;
  } catch (error) {
    if (generation === state.generation) {
      $('status').textContent = `Falha ao atualizar (${error.message}). Nova tentativa automática; os dados exibidos podem estar desatualizados.`;
    }
  } finally {
    state.busy = false;
    state.request = null;
    if (generation !== state.generation) refresh();
  }
}

function changeTimeSelection() {
  state.generation += 1;
  if (state.request) state.request.abort();
  $('minutes').disabled = !$('time-filter').checked;
  state.features = [];
  draw();
  $('period').textContent = $('time-filter').checked ? 'Aplicando filtro de tempo…' : 'Todo o histórico registrado';
  $('updated').textContent = 'Aguardando consulta';
  refresh();
}
$('time-filter').onchange = changeTimeSelection;
$('minutes').onchange = changeTimeSelection;
$('satellite').onchange = draw;
$('refresh').onclick = refresh;
async function poll() {
  await refresh();
  setTimeout(poll, refreshIntervalMs);
}
async function start() {
  try {
    const world = await getJSON('https://cdn.jsdelivr.net/npm/world-atlas@2.0.2/countries-110m.json');
    const countries = topojson.feature(world, world.objects.countries);
    const brazil = countries.features.find(f => Number(f.id) === 76);
    if (!brazil) throw new Error('Limite do Brasil ausente');
    path = d3.geoPath(d3.geoMercator().fitExtent([[45, 30], [855, 725]], brazil));
    d3.select('#land').selectAll('path').data(countries.features).join('path')
      .attr('class', f => Number(f.id) === 76 ? 'country brazil' : 'country').attr('d', path);
    d3.select('#brazil-clip path').attr('d', path(brazil));
    state.ready = true;
    poll();
  } catch (error) {
    $('status').textContent = `Erro ao carregar o mapa: ${error.message}. Verifique o acesso ao CDN e recarregue a página.`;
  }
}
start();
