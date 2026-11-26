const fs = require('node:fs');
const vm = require('node:vm');
const assert = require('node:assert/strict');
const context = {window: {}};
vm.runInNewContext(fs.readFileSync('frontend/data.js', 'utf8'), context);
const aggregate = features => JSON.parse(JSON.stringify(context.window.INPE_DATA.aggregateCells(features)));
function cluster(h3, satellite, count) {
  return {properties: {h3, satellite, count}};
}
const features = [
  cluster('abc', 'AQUA', 5),
  cluster('abc', 'TERRA', 1),
  cluster('def', 'TERRA', 4),
];
let cells = aggregate(features);
assert.equal(cells.length, 2);
assert.equal(cells[0].count, 6);
assert.deepEqual(cells[0].satellites, ['AQUA', 'TERRA']);
const filtered = context.window.INPE_DATA.aggregateCells(features, 'AQUA');
assert.equal(filtered.length, 1);
assert.equal(filtered[0].count, 5);
// Cada atualização substitui os dados: não acumula a mesma resposta duas vezes.
assert.equal(aggregate(features)[0].count, 6);
assert.equal(aggregate(features.slice(1))[0].count, 1);
assert.deepEqual(aggregate([]), []);
console.log('Agregação H3: totais e satélites validados.');
