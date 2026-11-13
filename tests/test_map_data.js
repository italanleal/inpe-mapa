const fs = require('node:fs');
const vm = require('node:vm');
const assert = require('node:assert/strict');
const context = {window: {}};
vm.runInNewContext(fs.readFileSync('frontend/data.js', 'utf8'), context);
const aggregate = features => JSON.parse(JSON.stringify(context.window.INPE_DATA.aggregateCells(features)));
function observation(h3, satellite, date, count) {
  return {properties: {h3, satellite, count, first_observed_at: date, last_observed_at: date}};
}
const features = [observation('abc', 'AQUA', '2026-11-12T15:40:00Z', 2),
  observation('abc', 'AQUA', '2026-11-13T16:40:00Z', 3),
  observation('abc', 'TERRA', '2026-11-13T16:45:00Z', 1),
  observation('def', 'TERRA', '2026-11-13T16:45:00Z', 4)];
let cells = aggregate(features);
assert.equal(cells.length, 2);
assert.equal(cells[0].count, 6);
assert.equal(cells[0].first, '2026-11-12T15:40:00Z');
assert.equal(cells[0].last, '2026-11-13T16:45:00Z');
assert.deepEqual(cells[0].satellites, ['AQUA', 'TERRA']);
const filtered = context.window.INPE_DATA.aggregateCells(features, 'AQUA');
assert.equal(filtered.length, 1);
assert.equal(filtered[0].count, 5);
// Cada atualização substitui os dados: não acumula a mesma resposta duas vezes.
assert.equal(aggregate(features)[0].count, 6);
assert.equal(aggregate(features.slice(2))[0].count, 1);
assert.deepEqual(aggregate([]), []);
console.log('Agregação H3: totais, satélites, dias distintos e expiração validados.');
