/* Agregação compartilhada pela visualização e pelos testes, sem dependência de D3. */
'use strict';
window.INPE_DATA = {
  aggregateCells(features, satellite = '') {
    const cells = new Map();
    for (const feature of features) {
      const props = feature.properties;
      if (satellite && props.satellite !== satellite) continue;
      let cell = cells.get(props.h3);
      if (!cell) {
        cell = {h3: props.h3, count: 0, satellites: new Set(), feature};
        cells.set(props.h3, cell);
      }
      cell.count += props.count;
      cell.satellites.add(props.satellite);
    }
    return [...cells.values()].map(cell => ({...cell, satellites: [...cell.satellites].sort()}));
  }
};
