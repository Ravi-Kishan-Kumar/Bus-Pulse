const state = { maps: {}, mapAdmin: {}, buses: [], adminRoutes: [], user: null,
  selectedRouteId: null, selectedRoute: null, startStop: null, destinationStop: null,
  pickerTarget: 'start', mapPickTarget: null, socket: null, pendingVehicleSnapshot: null, activeView: 'commuter', selectedBusId: null,
  lastBusRefresh: 0, regionId: 'whole_city', selectedRegion: 'whole_city', regionSelectionManual: false,
  regions: [], routeGeometry: {}, selectionMarkers: {}, journeySort: 'fastest', lastBusRender: 0, simulationRunning: false,
  selectedRouteIds: [], journeyOptions: [], commuterDemandProfile: 'normal', extraBuses: [], trackedExtraBusId: null, advisoriesLoading: false };
const $ = (selector) => document.querySelector(selector);
const $$ = (selector) => [...document.querySelectorAll(selector)];
const crowd = (pct = 0) => pct < 50 ? 'Low' : pct < 70 ? 'Medium' : pct <= 85 ? 'High' : 'Full';
const pct = (value) => `${Number(value || 0).toFixed(0)}%`;
const scenarioLabel = (profile) => ({ normal: 'Normal hours', moderate: 'Moderate hours', rush: 'Rush hour' }[profile] || 'Normal hours');

async function api(path, options = {}) {
  const response = await fetch(path, { ...options, credentials: 'same-origin' });
  if (response.status === 401) { location.assign('/'); throw new Error('Your session has expired. Please sign in again.'); }
  if (!response.ok) throw new Error((await response.json().catch(() => ({}))).detail || `Request failed (${response.status})`);
  return response.json();
}

function makeBlankStyle() {
  return { version: 8, sources: {}, layers: [{ id: 'blank', type: 'background', paint: { 'background-color': '#e9eef5' } }] };
}

function ensureNormalBusIcons(map) {
  const variants = [
    ['low', '#14a577'],
    ['moderate', '#e89b17'],
    ['full', '#df4655'],
  ];
  for (const [level, fill] of variants) {
    for (const moving of [true, false]) {
      const name = `bus-${level}-${moving ? 'moving' : 'stopped'}`;
      if (map.hasImage(name)) continue;
      const canvas = document.createElement('canvas');
      canvas.width = 48;
      canvas.height = 48;
      const context = canvas.getContext('2d');
      if (!context) continue;
      context.fillStyle = moving ? '#07845f' : '#526178';
      context.beginPath();
      context.roundRect(9, 3, 30, 42, 8);
      context.fill();
      context.fillStyle = fill;
      context.beginPath();
      context.roundRect(12, 5, 24, 38, 6);
      context.fill();
      context.fillStyle = '#dff6ff';
      context.beginPath();
      context.roundRect(15, 9, 18, 7, 2);
      context.fill();
      context.fillStyle = '#ffffff';
      context.fillRect(15, 20, 18, 2);
      context.fillStyle = '#dff6ff';
      context.beginPath();
      context.roundRect(15, 24, 18, 7, 2);
      context.fill();
      context.fillStyle = '#ffffff';
      context.fillRect(17, 35, 14, 3);
      context.fillStyle = '#202d3e';
      context.beginPath();
      context.roundRect(6, 11, 5, 9, 2);
      context.roundRect(37, 11, 5, 9, 2);
      context.roundRect(6, 31, 5, 9, 2);
      context.roundRect(37, 31, 5, 9, 2);
      context.fill();
      // MapLibre needs raw pixel data here. Passing a canvas works in some
      // builds but causes a "mismatched image size" error in the current
      // renderer, which prevented every vehicle layer from being installed.
      map.addImage(name, context.getImageData(0, 0, canvas.width, canvas.height), { pixelRatio: 2 });
    }
  }
  return Promise.resolve(true);
}

function ensureAdvisoryBusIcon(map) {
  if (map.hasImage('advisory-bus-diamond')) return Promise.resolve(true);
  const canvas = document.createElement('canvas');
  canvas.width = 56;
  canvas.height = 56;
  const context = canvas.getContext('2d');
  if (!context) return Promise.resolve(false);
  context.beginPath();
  context.moveTo(28, 3); context.lineTo(53, 28); context.lineTo(28, 53); context.lineTo(3, 28);
  context.closePath();
  context.fillStyle = '#7c3aed';
  context.fill();
  context.lineWidth = 4;
  context.strokeStyle = '#ffffff';
  context.stroke();
  context.fillStyle = '#ffffff';
  context.fillRect(20, 17, 16, 22);
  context.fillStyle = '#7c3aed';
  context.fillRect(23, 20, 10, 5);
  context.fillRect(23, 27, 10, 7);
  if (!map.hasImage('advisory-bus-diamond')) {
    map.addImage('advisory-bus-diamond', context.getImageData(0, 0, canvas.width, canvas.height), { pixelRatio: 2 });
  }
  return Promise.resolve(true);
}

function setupMap(id, isAdmin = false) {
  const mapEl = document.getElementById(id);
  if (!mapEl || !window.maplibregl) return null;
  const map = new maplibregl.Map({
    container: id,
    style: 'https://tiles.openfreemap.org/styles/liberty',
    center: [77.5946, 12.9716], zoom: 10.2, maxZoom: 18,
    attributionControl: true,
  });
  map.addControl(new maplibregl.NavigationControl({ showCompass: false }), 'top-right');
  let styleReady = false;
  let fallback = false;
  const attachLayers = async () => {
    if (styleReady) return;
    styleReady = true;
    try {
      const geo = isAdmin
        ? await api(`/api/network/geojson?usable_only=true&simulated_only=true&region_id=${encodeURIComponent(state.selectedRegion)}`)
        : { type: 'FeatureCollection', features: [] };
      if (!map.getSource('route-lines')) map.addSource('route-lines', { type: 'geojson', data: geo });
      cacheRouteGeometry(geo);
      if (isAdmin) {
        const region = state.regions.find((item) => item.id === state.selectedRegion);
        if (region) map.jumpTo({ center: region.center, zoom: region.zoom });
        else fitMapToRouteGeo(map, geo);
      }
      if (!map.getLayer('route-lines')) map.addLayer({
        id: 'route-lines', type: 'line', source: 'route-lines',
        paint: {
          'line-color': isAdmin ? ['case', ['==', ['get', 'simulated'], true], '#2865da', '#7a91b2']
            : ['match', ['get', 'journey_leg'], 0, '#2865da', 1, '#e36a45', '#2865da'],
          'line-opacity': isAdmin ? ['case', ['==', ['get', 'simulated'], true], 0.82, 0.45] : 0.95,
          'line-width': isAdmin ? ['case', ['==', ['get', 'simulated'], true], 2.5, 1.4] : 4,
        },
      });
      if (!map.getSource('vehicle-points')) map.addSource('vehicle-points', {
        type: 'geojson', data: { type: 'FeatureCollection', features: [] },
      });
      // A native circle layer is intentionally installed before custom icons.
      // It keeps every live vehicle visible if an individual browser rejects a
      // generated canvas image, while the bus and diamond symbols render on
      // top wherever custom images are supported.
      let busIconsReady = true;
      try {
        await ensureNormalBusIcons(map);
        await ensureAdvisoryBusIcon(map);
      } catch (error) {
        busIconsReady = false;
        console.warn('Using the native vehicle-marker fallback', error);
      }
      if (!map.getSource('region-area')) map.addSource('region-area', { type: 'geojson', data: regionGeoJSON(state.selectedRegion) });
      if (!map.getLayer('region-area-fill')) map.addLayer({ id: 'region-area-fill', type: 'fill', source: 'region-area',
        paint: { 'fill-color': '#4b83dc', 'fill-opacity': 0.045 } });
      if (!map.getLayer('region-area-line')) map.addLayer({ id: 'region-area-line', type: 'line', source: 'region-area',
        paint: { 'line-color': '#4b83dc', 'line-opacity': 0.65, 'line-width': 1.5, 'line-dasharray': [2, 2] } });
      if (!map.getSource('journey-points')) map.addSource('journey-points', {
        type: 'geojson', data: { type: 'FeatureCollection', features: [] },
      });
      if (!map.getLayer('journey-points')) map.addLayer({ id: 'journey-points', type: 'circle', source: 'journey-points',
        paint: { 'circle-radius': 7, 'circle-color': ['match', ['get', 'kind'], 'start', '#08a876', 'transfer', '#7c3aed', '#e36a45'],
          'circle-stroke-color': '#ffffff', 'circle-stroke-width': 2 } });
      map.moveLayer('region-area-fill', 'route-lines');
      if (!map.getLayer('vehicle-fallback-points')) map.addLayer({
        id: 'vehicle-fallback-points', type: 'circle', source: 'vehicle-points',
        paint: {
          'circle-radius': ['interpolate', ['linear'], ['zoom'], 8, 3.5, 10, 4.5, 13, 6, 16, 7.5],
          'circle-color': ['case', ['==', ['get', 'advisory_extra'], true], '#7c3aed',
            ['>=', ['get', 'occ'], 85], '#df4655', ['>=', ['get', 'occ'], 70], '#e89b17', '#14a577'],
          'circle-stroke-color': '#ffffff', 'circle-stroke-width': 1.5, 'circle-opacity': 0.98,
        },
      });
      map.moveLayer('region-area-line', 'vehicle-fallback-points');
      if (busIconsReady && !map.getLayer('vehicle-points')) map.addLayer({
        id: 'vehicle-points', type: 'symbol', source: 'vehicle-points',
        filter: ['!=', ['get', 'advisory_extra'], true],
        layout: {
          'icon-image': ['case',
            ['>=', ['get', 'occ'], 85], ['case', ['>', ['get', 'speed_mps'], 0.5], 'bus-full-moving', 'bus-full-stopped'],
            ['>=', ['get', 'occ'], 70], ['case', ['>', ['get', 'speed_mps'], 0.5], 'bus-moderate-moving', 'bus-moderate-stopped'],
            ['case', ['>', ['get', 'speed_mps'], 0.5], 'bus-low-moving', 'bus-low-stopped']],
          'icon-size': ['interpolate', ['linear'], ['zoom'], 8, 0.62, 10, 0.8, 13, 1.0, 16, 1.12],
          'icon-allow-overlap': true,
          'icon-ignore-placement': true,
          'icon-padding': 1,
        },
      });
      if (map.getLayer('vehicle-points')) map.moveLayer('region-area-line', 'vehicle-points');
      if (busIconsReady && !map.getLayer('vehicle-advisory')) map.addLayer({
        id: 'vehicle-advisory', type: 'symbol', source: 'vehicle-points',
        filter: ['==', ['get', 'advisory_extra'], true],
        layout: { 'icon-image': 'advisory-bus-diamond', 'icon-size': 0.82,
          'icon-allow-overlap': true, 'icon-ignore-placement': true },
      });
      if (!isAdmin && state.selectedRouteId) {
        const selectedGeo = await api(`/api/network/geojson?usable_only=true&route_id=${encodeURIComponent(state.selectedRouteId)}`);
        map.getSource('route-lines')?.setData(selectedGeo);
      }
      const handleVehicleClick = (event) => {
        if (state.mapPickTarget) { setMapJourneyStop(state.mapPickTarget, event.lngLat); return; }
        const bus = event.features?.[0]?.properties;
        if (bus?.vehicle_id) selectBus(bus.vehicle_id);
      };
      for (const vehicleLayer of ['vehicle-fallback-points', 'vehicle-points', 'vehicle-advisory']) {
        if (!map.getLayer(vehicleLayer)) continue;
        map.on('click', vehicleLayer, handleVehicleClick);
        map.on('mouseenter', vehicleLayer, () => { map.getCanvas().style.cursor = 'pointer'; });
        map.on('mouseleave', vehicleLayer, () => { map.getCanvas().style.cursor = ''; });
      }
      map.on('click', 'route-lines', (event) => {
        if (state.mapPickTarget) { setMapJourneyStop(state.mapPickTarget, event.lngLat); return; }
        const props = event.features?.[0]?.properties;
        if (props?.route_id) showRoute(props.route_id);
      });
      map.on('click', (event) => {
        if (state.mapPickTarget) setMapJourneyStop(state.mapPickTarget, event.lngLat);
      });
      map.on('moveend', renderBusPositionLayers);
      if (state.buses.length) renderBusPositionLayers();
      if (state.buses.length) updateBusLayers(state.buses);
    } catch (error) {
      console.error('Could not load route geometry', error);
    }
  };
  map.on('load', attachLayers);
  map.on('error', () => {
    if (!styleReady && !fallback) {
      fallback = true;
      map.setStyle(makeBlankStyle());
      map.once('style.load', attachLayers);
      const note = $('#map-fallback');
      if (note && !isAdmin) note.hidden = false;
    }
  });
  state.maps[id] = map;
  state.mapAdmin[id] = isAdmin;
  return map;
}

function updateBusLayers(buses, snapshot = false) {
  const incoming = buses || [];
  if (snapshot) state.buses = incoming;
  else {
    const byId = new Map(state.buses.map((bus) => [bus.vehicle_id, bus]));
    incoming.forEach((bus) => byId.set(bus.vehicle_id, { ...byId.get(bus.vehicle_id), ...bus }));
    state.buses = [...byId.values()];
  }
  renderBusPositionLayers();
  $('#metric-buses').textContent = state.buses.length;
  updateSelectedRouteBusCards();
  renderAdminExtraBuses();
  if (state.user?.role !== 'admin' && state.selectedRoute) {
    const routeIds = state.selectedRouteIds.length ? state.selectedRouteIds : [state.selectedRouteId].filter(Boolean);
    const routeBus = state.buses.find((bus) => routeIds.includes(String(bus.route_id)));
    if (routeBus && !state.selectedBusId) selectBus(routeBus.vehicle_id);
    const feedback = $('#commuter-demo-feedback');
    if (routeBus && feedback?.dataset.waiting === 'true') {
      const count = state.buses.filter((bus) => routeIds.includes(String(bus.route_id))).length;
      feedback.dataset.waiting = 'false';
      feedback.textContent = `Live stream connected · ${count} bus${count === 1 ? '' : 'es'} on the selected route. Choose a bus card or map marker to view details.`;
    }
  }
}

function renderBusPositionLayers() {
  const dataFor = (buses) => ({
    type: 'FeatureCollection',
    features: buses.filter((bus) => Number.isFinite(Number(bus.lon)) && Number.isFinite(Number(bus.lat))).map((bus) => ({
      type: 'Feature', properties: { ...bus, occ: Number(bus.occ_est_pct || 0) },
      geometry: { type: 'Point', coordinates: [Number(bus.lon), Number(bus.lat)] },
    })),
  });
  for (const [id, map] of Object.entries(state.maps)) {
    if ((state.mapAdmin[id] && state.activeView !== 'admin')
        || (!state.mapAdmin[id] && state.activeView !== 'commuter')) continue;
    if (!map?.getSource('vehicle-points')) continue;
    const selectedRoutes = state.selectedRouteIds.length ? state.selectedRouteIds : [state.selectedRouteId].filter(Boolean);
    const routeBuses = state.mapAdmin[id] ? state.buses : state.buses.filter((bus) => selectedRoutes.includes(String(bus.route_id)));
    const bounds = map.getBounds?.();
    const visibleBuses = routeBuses.filter((bus) => {
      const geometry = state.routeGeometry[`${bus.route_id}:${bus.direction_id}`];
      if (geometry && Number(bus.speed_mps) > 0.5) {
        const eventAt = Date.parse(bus.updated_at || bus.event_ts || new Date().toISOString());
        const age = Math.min(30, Math.max(0, (Date.now() - eventAt) / 1000));
        const progress = Math.min(geometry.routeLength, Number(bus.progress_m || 0) + Number(bus.speed_mps) * age);
        const [lon, lat] = routePosition(geometry, progress);
        bus.lon = lon;
        bus.lat = lat;
      }
      return !bounds || bounds.contains([Number(bus.lon), Number(bus.lat)]);
    });
    map.getSource('vehicle-points').setData(dataFor(visibleBuses));
  }
}

function regionGeoJSON(regionId) {
  const region = state.regions.find((item) => item.id === regionId);
  if (!region?.polygon) return { type: 'FeatureCollection', features: [] };
  const coordinates = [...region.polygon, region.polygon[0]];
  return { type: 'FeatureCollection', features: [{ type: 'Feature', properties: { region_id: regionId },
    geometry: { type: 'Polygon', coordinates: [coordinates] } }] };
}

function cacheRouteGeometry(geo) {
  for (const feature of geo?.features || []) {
    if (feature.geometry?.type !== 'LineString') continue;
    const coordinates = feature.geometry.coordinates || [];
    if (coordinates.length < 2) continue;
    const cumulative = [0];
    for (let index = 1; index < coordinates.length; index++) {
      const [lon1, lat1] = coordinates[index-1], [lon2, lat2] = coordinates[index];
      const lat = (lat1 + lat2) * Math.PI / 360;
      const dx = (lon2-lon1) * 111320 * Math.cos(lat), dy = (lat2-lat1) * 111320;
      cumulative.push(cumulative[cumulative.length-1] + Math.hypot(dx, dy));
    }
    state.routeGeometry[`${feature.properties.route_id}:${feature.properties.direction_id}`] = {
      coordinates, cumulative, shapeLength: cumulative.at(-1), routeLength: Number(feature.properties.length_m || cumulative.at(-1)),
    };
  }
}

function routePosition(geometry, progress) {
  const target = Math.max(0, Math.min(geometry.shapeLength, progress / Math.max(1, geometry.routeLength) * geometry.shapeLength));
  let low = 1, high = geometry.cumulative.length-1;
  while (low < high) {
    const mid = (low+high) >> 1;
    if (geometry.cumulative[mid] < target) low = mid + 1; else high = mid;
  }
  const index = low, segment = geometry.cumulative[index]-geometry.cumulative[index-1];
  const ratio = segment > 0 ? (target-geometry.cumulative[index-1])/segment : 0;
  const a = geometry.coordinates[index-1], b = geometry.coordinates[index];
  return [a[0]+(b[0]-a[0])*ratio, a[1]+(b[1]-a[1])*ratio];
}

function fitMapToRouteGeo(map, geo) {
  if (!map || !window.maplibregl || !geo?.features?.length) return false;
  const bounds = new maplibregl.LngLatBounds();
  geo.features.forEach((feature) => (feature.geometry?.coordinates || []).forEach((point) => bounds.extend(point)));
  if (!bounds.isEmpty()) map.fitBounds(bounds, { padding: 48, maxZoom: 12.5, duration: 650 });
  return !bounds.isEmpty();
}

async function loadHealth() {
  try {
    const health = await api('/api/health');
    const simulation = health.simulation || {};
    state.simulationRunning = Boolean(simulation.running);
    const previousRegion = state.regionId;
    state.regionId = simulation.region_id || 'whole_city';
    state.regions = simulation.regions || state.regions;
    const commuterBanner = $('#commuter-simulation-banner');
    if (commuterBanner) {
      commuterBanner.classList.toggle('running', Boolean(simulation.running));
      commuterBanner.classList.toggle('paused', !simulation.running);
      commuterBanner.innerHTML = `<i></i><span>${simulation.running
        ? `<b>${escapeHtml(simulation.region_name || 'Bengaluru')}</b> simulation running · ${scenarioLabel(simulation.demand_profile)}`
        : `Simulation paused · waiting for the administrator to start ${escapeHtml(simulation.region_name || 'a region')}`}</span>`;
    }
    const pulse = $('#simulation-pulse');
    if (pulse) {
      pulse.classList.toggle('running', Boolean(simulation.running));
      pulse.classList.toggle('paused', !simulation.running);
      pulse.querySelector('small').textContent = simulation.running
        ? `${simulation.region_name || 'Region'} · ${scenarioLabel(simulation.demand_profile)}` : 'Simulation paused';
      pulse.querySelector('b').textContent = simulation.running ? 'Live' : 'Pulse';
    }
    const scenarioAction = { normal: 'normal_hours', moderate: 'moderate_hours', rush: 'rush_hour' }[simulation.demand_profile];
    $$('[data-action="normal_hours"], [data-action="moderate_hours"], [data-action="rush_hour"]')
      .forEach((button) => button.classList.toggle('active', Boolean(simulation.running && button.dataset.action === scenarioAction)));
    if (!state.regionSelectionManual && $('#simulation-region')) {
      state.selectedRegion = simulation.region_id || 'whole_city';
      $('#simulation-region').value = state.selectedRegion;
    }
    if (previousRegion !== state.regionId && state.user?.role !== 'admin') {
      const commuterMap = state.maps['commuter-map'];
      if (commuterMap?.getSource('region-area')) commuterMap.getSource('region-area').setData(regionGeoJSON(state.regionId));
    }
    $('#system-status').textContent = simulation.running
      ? `${simulation.region_name} · Running`
      : `${simulation.region_name || 'Bengaluru'} · Paused`;
    if ($('#sim-state')) {
      $('#sim-state').textContent = simulation.running
        ? `${simulation.region_name} · ${scenarioLabel(simulation.demand_profile)}` : `${simulation.region_name || 'Bengaluru'} · Paused`;
      $('#sim-state').className = `badge ${simulation.running ? 'low' : 'neutral'}`;
    }
    if ($('#pipeline-badge')) {
      $('#pipeline-badge').textContent = 'Database ready';
      $('#pipeline-badge').className = 'badge low';
    }
    if ($('#metric-routes')) $('#metric-routes').textContent = Number(health.route_count).toLocaleString();
    $('#route-count').textContent = `${Number(health.route_count).toLocaleString()} routes`;
  } catch (error) {
    $('#system-status').textContent = 'Waiting for local services';
    if ($('#pipeline-badge')) {
      $('#pipeline-badge').textContent = 'Waiting';
      $('#pipeline-badge').className = 'badge neutral';
    }
  }
}

async function connectVehicleStream() {
  if (state.socket && state.socket.readyState < WebSocket.CLOSING) state.socket.close();
  state.pendingVehicleSnapshot = null;
  const commuterRouteIds = state.selectedRouteIds.length ? state.selectedRouteIds : [state.selectedRouteId].filter(Boolean);
  if (state.user?.role !== 'admin' && !commuterRouteIds.length) {
    state.socket = null;
    state.buses = [];
    renderBusPositionLayers();
    return;
  }
  const protocol = location.protocol === 'https:' ? 'wss:' : 'ws:';
  const params = new URLSearchParams();
  if (state.user?.role !== 'admin') {
    params.set('route_ids', commuterRouteIds.join(','));
    params.set('region_id', state.regionId || 'whole_city');
  }
  else params.set('region_id', state.selectedRegion || 'whole_city');
  state.buses = [];
  renderBusPositionLayers();
  const socket = new WebSocket(`${protocol}//${location.host}/ws${params.size ? `?${params}` : ''}`);
  state.socket = socket;
  socket.onmessage = (message) => {
    try {
      const payload = JSON.parse(message.data);
      if (payload.type === 'vehicles') {
        const chunkCount = Number(payload.snapshot_chunk_count || 1);
        if (chunkCount > 1) {
          const chunkIndex = Number(payload.snapshot_chunk_index || 0);
          if (chunkIndex === 0 || !state.pendingVehicleSnapshot) {
            state.pendingVehicleSnapshot = { snapshot: Boolean(payload.snapshot), expected: chunkCount, next: 0, buses: [] };
          }
          const pending = state.pendingVehicleSnapshot;
          if (pending.expected !== chunkCount || pending.next !== chunkIndex) {
            state.pendingVehicleSnapshot = null;
            return;
          }
          pending.buses.push(...(payload.buses || []));
          pending.next += 1;
          if (pending.next === pending.expected) {
            updateBusLayers(pending.buses, pending.snapshot);
            state.pendingVehicleSnapshot = null;
          }
        } else {
          updateBusLayers(payload.buses, payload.snapshot);
        }
        if (state.selectedBusId && payload.buses.some((bus) => bus.vehicle_id === state.selectedBusId)
            && Date.now()-state.lastBusRefresh > 15000) selectBus(state.selectedBusId);
      }
    } catch (error) { console.warn('Bad stream update', error); }
  };
  socket.onclose = () => { if (state.socket === socket) setTimeout(connectVehicleStream, 2500); };
}

function renderRoutes(routes) {
  const host = $('#route-results');
  state.journeyOptions = routes;
  $('#route-count').textContent = `${routes.length} option${routes.length === 1 ? '' : 's'}`;
  if (!routes.length) { host.innerHTML = '<p class="empty-state">No matching BMTC routes.</p>'; return; }
  host.innerHTML = routes.slice(0, 80).map((route, index) => {
    if (Number(route.transfer_count) > 0) {
      const legs = route.legs || [];
      const path = legs.map((leg) => `${escapeHtml(leg.short_name || leg.route_id)} (${(leg.journey_stops || []).length} stops)`).join(' → ');
      const stopDetails = legs.map((leg, legIndex) => `<li><b>Bus ${legIndex + 1}: ${escapeHtml(leg.short_name || leg.route_id)}</b><ol>${(leg.journey_stops || []).map((stop) => `<li>${escapeHtml(stop.name)}</li>`).join('')}</ol></li>`).join('');
      const nearest = legs[0]?.nearest_bus;
      const status = nearest ? `Bus ${escapeHtml(nearest.vehicle_id)} · ${Number(nearest.eta_to_board_min).toFixed(0)} min to board · ${pct(nearest.occ_est_pct)}` : 'No live bus snapshot';
      return `<div class="route-row transfer-route-row"><div class="route-info"><b>${path}</b><small>1 transfer at ${escapeHtml(route.transfer_stop?.name || route.transfer_stop_name)} · ${Number(route.journey_distance_km).toFixed(1)} km · ~${Number(route.estimated_total_min).toFixed(0)} min · ${status}</small><details class="journey-stop-details"><summary>Show both bus legs and stops</summary><ol>${stopDetails}</ol></details></div><div class="route-actions"><button class="route-action" data-journey-option="${index}">View trip</button></div></div>`;
    }
    const stops = route.journey_stops || [];
    const stopPreview = stops.length
      ? `<details class="journey-stop-details"><summary>${stops.length} stops on this segment</summary><ol>${stops.map((stop) => `<li>${escapeHtml(stop.name)}</li>`).join('')}</ol></details>`
      : '';
    const bus = route.nearest_bus;
    const live = bus ? `Nearest ${escapeHtml(bus.vehicle_id)} · ${Number(bus.eta_to_board_min).toFixed(0)} min · ${pct(bus.occ_est_pct)}`
      : (Number(route.live_buses || 0) ? `${route.live_buses} live simulated` : (route.simulated ? 'no current bus snapshot' : 'route map only'));
    const duration = route.estimated_total_min ? ` · ~${Number(route.estimated_total_min).toFixed(0)} min total` : '';
    return `<div class="route-row"><div class="route-info"><b>${escapeHtml(route.short_name || route.route_id)} <span class="muted">· ${live}</span></b><small>${escapeHtml(route.long_name || 'BMTC route')}${duration}</small>${stopPreview}</div>
    <div class="route-actions">${bus ? `<button class="route-action secondary-route-action" data-bus="${escapeHtml(bus.vehicle_id)}">Bus</button>` : ''}<button class="route-action" data-route="${escapeHtml(route.route_id)}">${route.simulated ? 'Use route' : 'View route'}</button></div></div>`;
  }).join('');
}

async function searchRoutes() {
  const q = $('#route-search').value.trim();
  if (!q) return;
  try {
    const result = await api(`/api/network/routes?q=${encodeURIComponent(q)}&usable_only=true&region_id=${encodeURIComponent(state.regionId)}`);
    renderRoutes(result.routes);
  } catch (error) { $('#route-results').innerHTML = `<p class="empty-state">${escapeHtml(error.message)}</p>`; }
}

function openStopPicker(target) {
  clearJourneyRouteSelection();
  state.pickerTarget = target;
  $('#picker-title').textContent = target === 'start' ? 'CHOOSE STARTING STOP' : 'CHOOSE DESTINATION STOP';
  $('#stop-picker').hidden = false;
  $('#stop-search').value = '';
  $('#stop-results').innerHTML = '<p class="empty-state">Type at least two characters to search stops.</p>';
  $('#stop-search').focus();
}

async function searchStops() {
  const query = $('#stop-search').value.trim();
  if (query.length < 2) {
    $('#stop-results').innerHTML = '<p class="empty-state">Type at least two characters to search stops.</p>';
    return;
  }
  try {
    const result = await api(`/api/network/stops?q=${encodeURIComponent(query)}&limit=40`);
    $('#stop-results').innerHTML = result.stops.length ? result.stops.map((stop) => `
      <button class="stop-result" data-stop-id="${escapeHtml(stop.stop_id)}" data-stop-name="${escapeHtml(stop.name)}">
        <span class="stop-pin">●</span><span><b>${escapeHtml(stop.name || 'Unnamed stop')}</b><small>${escapeHtml(stop.stop_id)} · ${Number(stop.route_count || 0)} routes</small></span><span class="stop-pick-label">Choose</span>
      </button>`).join('') : '<p class="empty-state">No stops found. Try another name.</p>';
  } catch (error) {
    $('#stop-results').innerHTML = `<p class="empty-state">${escapeHtml(error.message)}</p>`;
  }
}

async function resolveGpsStop(target) {
  const input = $(`#${target}-coordinates`);
  const raw = input.value.trim();
  if (!raw) return state[target === 'start' ? 'startStop' : 'destinationStop'];
  const match = raw.match(/^\s*(-?\d+(?:\.\d+)?)\s*,\s*(-?\d+(?:\.\d+)?)\s*$/);
  if (!match) throw new Error(`Enter ${target} as latitude, longitude, for example 12.9716, 77.5946.`);
  const lat = Number(match[1]), lon = Number(match[2]);
  if (!Number.isFinite(lat) || !Number.isFinite(lon) || lat < 12.6 || lat > 13.4 || lon < 77.2 || lon > 78.1) {
    throw new Error('Coordinates must be within the Bengaluru demo area (lat 12.6–13.4, lon 77.2–78.1).');
  }
  const result = await api(`/api/network/stops/nearby?lat=${lat}&lon=${lon}&limit=1`);
  const stop = result.stops?.[0];
  if (!stop) throw new Error(`No GTFS stop found near the ${target} coordinates.`);
  const chosen = { ...stop, coordinate_match: true };
  if (target === 'start') state.startStop = chosen;
  else state.destinationStop = chosen;
  const map = state.maps['commuter-map'];
  if (map && window.maplibregl) {
    state.selectionMarkers[target]?.remove();
    state.selectionMarkers[target] = new maplibregl.Marker({ color: target === 'start' ? '#08a876' : '#e36a45' })
      .setLngLat([Number(stop.lon), Number(stop.lat)])
      .setPopup(new maplibregl.Popup({ offset: 16 }).setText(`Nearest stop: ${stop.name}`)).addTo(map);
  }
  $(`#${target}-stop-label`).textContent = `Near ${stop.name} · ${(Number(stop.distance_km) * 1000).toFixed(0)} m`;
  return chosen;
}

async function findJourneyRoutes() {
  clearJourneyRouteSelection();
  try {
    const start = await resolveGpsStop('start');
    const destination = await resolveGpsStop('destination');
    if (!start || !destination) {
      $('#route-results').innerHTML = '<p class="empty-state">Choose both stops, enter both coordinate pairs, or pick the locations on the map.</p>';
      return;
    }
    const query = new URLSearchParams({ from_stop_id: state.startStop.stop_id, to_stop_id: state.destinationStop.stop_id,
      sort_by: state.journeySort });
    const result = await api(`/api/journey/routes?${query}`);
    $('#route-panel-title').textContent = 'Routes for your journey';
    $('#route-count').textContent = `${result.routes.length} option${result.routes.length === 1 ? '' : 's'}`;
    if (!result.routes.length) {
      $('#route-results').innerHTML = '<p class="empty-state">No single BMTC route serves these stops in that direction. Try swapping the stops or searching a route directly.</p>';
      return;
    }
    renderRoutes(result.routes);
  } catch (error) {
    $('#route-results').innerHTML = `<p class="empty-state">${escapeHtml(error.message)}</p>`;
  }
}

function chooseStop(stopId, stopName) {
  const stop = { stop_id: stopId, name: stopName };
  if (state.pickerTarget === 'start') {
    state.startStop = stop;
    $('#start-coordinates').value = '';
    $('#start-stop-label').textContent = stop.name;
  } else {
    state.destinationStop = stop;
    $('#destination-coordinates').value = '';
    $('#destination-stop-label').textContent = stop.name;
  }
  $('#stop-picker').hidden = true;
  if (state.startStop && state.destinationStop) findJourneyRoutes();
}

function setMapPickMode(target) {
  clearJourneyRouteSelection();
  state.mapPickTarget = target;
  state.pickerTarget = target;
  $('#stop-picker').hidden = true;
  $('#map-pick-feedback').textContent = `Click the map to set your ${target} location.`;
  const map = state.maps['commuter-map'];
  if (map) map.getCanvas().style.cursor = 'crosshair';
}

async function setMapJourneyStop(target, point) {
  state.mapPickTarget = null;
  const map = state.maps['commuter-map'];
  if (map) map.getCanvas().style.cursor = '';
  $('#map-pick-feedback').textContent = 'Finding the nearest BMTC stop…';
  try {
    const result = await api(`/api/network/stops/nearby?lat=${point.lat}&lon=${point.lng}&limit=8`);
    const stop = result.stops?.find((item) => Number(item.route_count) > 0) || result.stops?.[0];
    if (!stop) throw new Error('No stops found near that map point. Try a nearby road or stop.');
    state.selectionMarkers[target]?.remove();
    state.selectionMarkers[target] = new maplibregl.Marker({ color: target === 'start' ? '#08a876' : '#e36a45' })
      .setLngLat([Number(stop.lon), Number(stop.lat)]).setPopup(new maplibregl.Popup({ offset: 16 }).setText(stop.name)).addTo(map);
    $('#map-pick-feedback').textContent = `${target === 'start' ? 'Start' : 'Destination'} set to ${stop.name} · ${(Number(stop.distance_km)*1000).toFixed(0)} m from your click`;
    chooseStop(stop.stop_id, stop.name);
  } catch (error) {
    $('#map-pick-feedback').textContent = error.message;
  }
}

function updateSelectedRouteBusCards() {
  const host = $('#route-bus-list');
  if (!host || !state.selectedRoute) return;
  const selectedRoutes = state.selectedRouteIds.length ? state.selectedRouteIds : [state.selectedRouteId].filter(Boolean);
  const routeBuses = state.buses.filter((bus) => selectedRoutes.includes(String(bus.route_id)));
  $('#route-count').textContent = `${routeBuses.length} live`;
  host.innerHTML = routeBuses.length ? routeBuses.map((bus) => {
    const moving = Number(bus.speed_mps || 0) > 0.5;
    return `<button class="live-bus-card" data-bus="${escapeHtml(bus.vehicle_id)}">
      <span class="live-bus-top"><b>${escapeHtml(bus.vehicle_id)} ${bus.advisory_extra ? '<em class="extra-bus-label">◆ Extra bus</em>' : ''}</b><span class="motion-state ${moving ? 'moving' : 'stopped'}"><i></i>${moving ? 'Moving' : 'Stopped'}</span></span>
      <span class="live-bus-meta">${moving ? `${(Number(bus.speed_mps) * 3.6).toFixed(0)} km/h` : 'Waiting at stop'} · ${crowd(bus.occ_est_pct)} · ${pct(bus.occ_est_pct)}</span>
      <span class="live-bus-position">${Number(bus.lat).toFixed(4)}, ${Number(bus.lon).toFixed(4)}</span>
    </button>`;
  }).join('') : '<p class="empty-state">No buses reporting on this route right now.</p>';
}

function syncCommuterDemoPanel() {
  const selected = state.selectedRoute;
  const label = $('#commuter-demo-route');
  const run = $('#commuter-run-demo');
  if (!label || !run) return;
  const eligible = Boolean(selected?.simulated && state.selectedRouteId);
  label.textContent = eligible
    ? `Selected route: ${selected.short_name || selected.route_id} · ${selected.long_name || 'BMTC route'}`
    : (state.selectedRouteIds.length > 1
      ? 'This is a transfer trip. Choose one simulated route from Search BMTC route number or name to run a route demo.'
      : 'Choose a simulated route from the results below.');
  run.disabled = !eligible;
}

function clearJourneyRouteSelection() {
  state.selectedRouteId = null;
  state.selectedRouteIds = [];
  state.selectedRoute = null;
  state.journeyOptions = [];
  state.selectedBusId = null;
  $('#map-route-name').textContent = 'Choose a route to view it';
  $('#route-panel-title').textContent = 'Choose a route';
  $('#route-count').textContent = '— routes';
  $('#route-results').innerHTML = '<p class="empty-state">Choose a route for this trip, or change either location above.</p>';
  $('#bus-title').textContent = 'Select a bus';
  $('#bus-crowd').textContent = 'No live data';
  $('#bus-crowd').className = 'badge neutral';
  $('#bus-detail').innerHTML = '<p class="empty-state">Choose a moving bus on the map or from a route card.</p>';
  const map = state.maps['commuter-map'];
  map?.getSource('route-lines')?.setData({ type: 'FeatureCollection', features: [] });
  map?.getSource('journey-points')?.setData({ type: 'FeatureCollection', features: [] });
  syncCommuterDemoPanel();
  if (state.user?.role !== 'admin') connectVehicleStream();
}

function resetJourney() {
  clearJourneyRouteSelection();
  state.startStop = null;
  state.destinationStop = null;
  state.selectionMarkers.start?.remove();
  state.selectionMarkers.destination?.remove();
  state.selectionMarkers = {};
  $('#start-stop-label').textContent = 'Choose starting stop';
  $('#destination-stop-label').textContent = 'Choose destination stop';
  $('#start-coordinates').value = '';
  $('#destination-coordinates').value = '';
  $('#map-pick-feedback').textContent = '';
  $('#route-results').innerHTML = '<p class="empty-state">Choose start and destination stops, or search for a route number.</p>';
  $('#stop-picker').hidden = true;
}

async function showJourneyOption(option) {
  if (!option) return;
  const legs = option.legs || [];
  if (!legs.length) return showRoute(option.route_id);
  state.selectedBusId = null;
  const demoLeg = legs.find((leg) => leg.simulated) || legs[0];
  state.selectedRouteIds = demoLeg?.simulated
    ? [String(demoLeg.route_id)]
    : [...new Set(legs.map((leg) => String(leg.route_id)))];
  state.selectedRouteId = String(demoLeg?.route_id || legs[0].route_id);
  state.selectedRoute = { route_id: state.selectedRouteId, short_name: demoLeg.short_name,
    long_name: demoLeg.long_name, simulated: Boolean(demoLeg.simulated) };
  syncCommuterDemoPanel();
  connectVehicleStream();
  try {
    const collections = await Promise.all(legs.map((leg) => api(
      `/api/network/geojson?usable_only=true&route_id=${encodeURIComponent(leg.route_id)}`)));
    const geo = { type: 'FeatureCollection', features: collections.flatMap((collection, legIndex) =>
      (collection.features || []).map((feature) => ({
        ...feature, properties: { ...feature.properties, journey_leg: legIndex },
      }))) };
    cacheRouteGeometry(geo);
    const map = state.maps['commuter-map'];
    const startStop = legs[0].journey_stops?.[0];
    const endStop = legs.at(-1).journey_stops?.at(-1);
    const points = [
      startStop && { kind: 'start', stop: startStop },
      option.transfer_stop && { kind: 'transfer', stop: option.transfer_stop },
      endStop && { kind: 'destination', stop: endStop },
    ].filter(Boolean).map(({ kind, stop }) => ({
      type: 'Feature', properties: { kind, name: stop.name },
      geometry: { type: 'Point', coordinates: [Number(stop.lon), Number(stop.lat)] },
    }));
    const applyMap = () => {
      if (!map?.getSource('route-lines')) return false;
      map.getSource('route-lines').setData(geo);
      map.getSource('journey-points')?.setData({ type: 'FeatureCollection', features: points });
      fitMapToRouteGeo(map, geo);
      return true;
    };
    const applyWhenReady = () => { if (!applyMap() && map) map.once('idle', applyWhenReady); };
    applyWhenReady();
    $('#map-route-name').textContent = legs.map((leg) => leg.short_name || leg.route_id).join(' → ');
    $('#route-panel-title').textContent = 'Transfer journey';
    $('#route-count').textContent = `1 transfer · ~${Number(option.estimated_total_min).toFixed(0)} min`;
    $('#route-results').innerHTML = `<div class="route-focus"><p class="eyebrow">TRANSFER ROUTE · ${Number(option.journey_distance_km).toFixed(1)} KM</p><h3>Change buses at ${escapeHtml(option.transfer_stop?.name || option.transfer_stop_name)}</h3>${legs.map((leg, index) => {
      const stops = leg.journey_stops || [];
      const bus = leg.nearest_bus;
      const live = bus ? `Nearest bus ${escapeHtml(bus.vehicle_id)} · ${Number(bus.eta_to_board_min).toFixed(0)} min · ${pct(bus.occ_est_pct)}` : 'No live bus snapshot for this leg';
      const selected = String(leg.route_id) === state.selectedRouteId;
      const demoButton = leg.simulated
        ? `<button class="route-action secondary-route-action" data-demo-leg="${index}" data-demo-route="${escapeHtml(leg.route_id)}" data-demo-short="${escapeHtml(leg.short_name || leg.route_id)}" data-demo-long="${escapeHtml(leg.long_name || '')}">${selected ? 'Selected demo leg' : 'Simulate this leg'}</button>`
        : '<small class="simulation-note">Static route · not available for a live demo</small>';
      return `<section class="transfer-leg"><b>Leg ${index + 1}: ${escapeHtml(leg.short_name || leg.route_id)}</b><small>${escapeHtml(leg.long_name || '')} · ${live}</small><details class="journey-stop-details"><summary>${stops.length} stops on this leg</summary><ol>${stops.map((stop) => `<li>${escapeHtml(stop.name)}</li>`).join('')}</ol></details>${demoButton}</section>`;
    }).join('')}<p class="simulation-note">The planner uses an estimated 8-minute transfer allowance. Route and transfer data come from the loaded GTFS network.</p><div id="route-bus-list" class="live-bus-list"></div></div>`;
    updateSelectedRouteBusCards();
  } catch (error) {
    $('#route-results').innerHTML = `<p class="empty-state">${escapeHtml(error.message)}</p>`;
  }
}

async function showRoute(routeId) {
  try {
    const result = await api(`/api/routes/${encodeURIComponent(routeId)}/summary`);
    const route = result.route;
    state.selectedBusId = null;
    state.selectedRouteId = routeId;
    state.selectedRouteIds = [routeId];
    state.selectedRoute = route;
    syncCommuterDemoPanel();
    connectVehicleStream();
    const geo = await api(`/api/network/geojson?usable_only=true&route_id=${encodeURIComponent(routeId)}`);
    const map = state.maps['commuter-map'];
    cacheRouteGeometry(geo);
    map?.getSource('route-lines')?.setData(geo);
    $('#map-route-name').textContent = `${route.short_name || route.route_id} · ${route.long_name || 'BMTC route'}`;
    if (geo.features.length && map && window.maplibregl) {
      const bounds = new maplibregl.LngLatBounds();
      geo.features.forEach((feature) => feature.geometry.coordinates.forEach((point) => bounds.extend(point)));
      map.fitBounds(bounds, { padding: 50, maxZoom: 13, duration: 700 });
    }
    $('#route-panel-title').textContent = `${route.short_name || route.route_id} buses`;
    $('#route-count').textContent = `${state.buses.filter((bus) => bus.route_id === routeId).length} live`;
    const stopList = result.stops || [];
    $('#route-results').innerHTML = `<div class="route-focus"><p class="eyebrow">${escapeHtml(route.short_name || route.route_id)} · ${result.simulated ? 'SIMULATED LIVE DATA' : 'STATIC GTFS ROUTE'}</p><h3>${escapeHtml(route.long_name || 'BMTC route')}</h3><details class="journey-stop-details"><summary>${stopList.length} stops on this route</summary><ol>${stopList.map((stop) => `<li>${escapeHtml(stop.name)}</li>`).join('')}</ol></details><p class="muted">Select a moving bus to view its current crowding, seats, standing and handhold use. Set the number of demo buses and crowding profile in the route demo panel above.</p>${route.simulated ? '' : '<p class="simulation-note">This route has static map data and is not included in the simulation fleet.</p>'}<div id="route-bus-list" class="live-bus-list"></div></div>`;
    updateSelectedRouteBusCards();
    if (route.simulated) {
      const bus = state.buses.find((item) => item.route_id === routeId) || result.buses?.[0];
      if (bus) selectBus(bus.vehicle_id);
    }
  } catch (error) { $('#route-results').innerHTML = `<p class="empty-state">${escapeHtml(error.message)}</p>`; }
}

async function startCommuterRouteDemo(routeId) {
  const feedback = $('#commuter-demo-feedback');
  const count = Number($('#commuter-bus-count')?.value || 2);
  const demandProfile = state.commuterDemandProfile || 'normal';
  routeId = routeId || state.selectedRouteId;
  if (!routeId || !state.selectedRoute?.simulated) {
    if (feedback) feedback.textContent = 'Select an eligible simulated BMTC route first.';
    return;
  }
  if (!Number.isInteger(count) || count < 1 || count > 25) {
    if (feedback) feedback.textContent = 'Choose a whole number from 1 to 25 buses.';
    return;
  }
  if (feedback) feedback.textContent = 'Starting the shared route simulation…';
  try {
    state.selectedBusId = null;
    state.buses = [];
    $('#bus-title').textContent = 'Starting route demo…';
    $('#bus-crowd').textContent = 'Waiting for live data';
    $('#bus-crowd').className = 'badge neutral';
    $('#bus-detail').innerHTML = '<p class="empty-state">The simulator is applying this route fleet. Bus positions and crowding will appear as soon as Kafka telemetry reaches the database.</p>';
    if (feedback) feedback.dataset.waiting = 'true';
    updateSelectedRouteBusCards();
    const result = await api('/api/commuter/simulation', {
      method: 'POST', headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ route_id: routeId, buses: count, region_id: state.regionId, demand_profile: demandProfile }),
    });
    if (feedback) feedback.textContent = `Command accepted · ${result.region_name} · ${result.applied_buses} buses requested. Waiting for the first live snapshot…`;
    state.regionId = result.region_id || state.regionId;
    state.selectedRouteId = routeId;
    state.selectedRouteIds = [routeId];
    if (state.selectedRoute) state.selectedRoute.route_id = routeId;
    connectVehicleStream();
    setTimeout(loadHealth, 800);
    setTimeout(() => {
      if (feedback?.dataset.waiting === 'true') {
        feedback.dataset.waiting = 'false';
        feedback.textContent = 'No bus snapshot has reached the commuter view yet. Check the dashboard service status, then retry the route demo if this persists.';
      }
    }, 20000);
  } catch (error) {
    if (feedback) feedback.textContent = error.message;
  }
}

async function selectBus(vehicleId) {
  try {
    state.selectedBusId = vehicleId;
    state.lastBusRefresh = Date.now();
    const result = await api(`/api/buses/${encodeURIComponent(vehicleId)}`);
    const bus = result.vehicle;
    const explain = result.prediction?.contributions || {};
    const explanation = Object.entries(explain)
      .filter(([key, value]) => key !== 'target_ts' && key !== 'horizon_minutes' && Number.isFinite(Number(value)))
      .sort((a, b) => Math.abs(Number(b[1])) - Math.abs(Number(a[1])))
      .slice(0, 3)
      .map(([key, value]) => `<span>${escapeHtml(key.replaceAll('_', ' '))}: ${Number(value) >= 0 ? '+' : ''}${Number(value).toFixed(1)}</span>`)
      .join('');
    const status = crowd(bus.occ_est_pct).toLowerCase();
    const moving = Number(bus.speed_mps || 0) > 0.5;
    const estimate = result.estimate || {};
    const predictedCount = estimate.onboard_low == null ? 'Warming up' : `${estimate.onboard_low}–${estimate.onboard_high} people`;
    $('#bus-title').textContent = `${bus.vehicle_id}${bus.advisory_extra ? ' · Advisory extra' : ''} · route ${bus.route_id}`;
    $('#bus-crowd').textContent = `${crowd(bus.occ_est_pct)} ${pct(bus.occ_est_pct)}`;
    $('#bus-crowd').className = `badge ${status}`;
    $('#bus-detail').innerHTML = `
      <div class="bus-motion ${moving ? 'moving' : 'stopped'}"><i></i><span><b>${moving ? 'Moving now' : 'Stopped / at stop'}</b><small>${moving ? `${(Number(bus.speed_mps) * 3.6).toFixed(0)} km/h` : 'Waiting for the next movement update'} · delay ${Math.max(0,Math.round(Number(bus.delay_s || 0) / 60))} min</small></span></div>
      <div class="bus-summary"><span>Current onboard estimate</span><b>${bus.onboard_count ?? (Number(bus.seated_count || 0) + Number(bus.standing_count || 0))} / ${bus.capacity || 70}</b></div>
      <div class="capacity-profile"><b>${escapeHtml(bus.vehicle_type || 'City bus · simulation')}</b><span>Nominal ${bus.capacity || 70} passengers · rough capacity ${bus.capacity_range_min || 60}–${bus.capacity_range_max || 75}</span></div>
      <div class="occupancy-bars"><div class="occupancy-label"><span>Estimated capacity</span><b>${pct(bus.occ_est_pct)}</b></div><div class="bar-track"><div class="bar-fill" style="width:${Math.min(100,Number(bus.occ_est_pct || 0))}%"></div></div></div>
      <div class="capacity-breakdown"><div class="capacity-stat"><small>Seated</small><b>${bus.seated_count || 0}</b></div><div class="capacity-stat"><small>Standing</small><b>${bus.standing_count || 0}</b></div><div class="capacity-stat"><small>Using handholds</small><b>${bus.supported_standing_count || 0}</b></div></div>
      <p class="simulation-note">Synthetic class and capacity assumptions, not official BMTC specifications or onboard sensor readings. ${bus.seat_capacity || 0} seats · ${bus.stand_capacity || 0} standing spaces · ${bus.support_capacity || 0} handhold positions.</p>
      <div class="forecast-box"><span>+5 min occupancy prediction</span><b>${result.prediction ? pct(result.prediction.pred_pct) : 'Warming up'}</b></div>
      <div class="forecast-range"><span>Trend-based onboard range</span><b>${predictedCount}</b><small>${escapeHtml(estimate.label || 'rough simulated range')}</small></div>
      ${explanation ? `<p class="forecast-explanation"><b>${escapeHtml(result.prediction.model_version)}</b> · largest feature contributions ${explanation}</p>` : ''}
      <div class="trend-mini">${(result.trend || []).map((p) => `<i title="${pct(p.occ_mean)}" style="height:${Math.max(7,Math.min(100,Number(p.occ_mean || 0)))}%"></i>`).join('') || '<span class="muted">Trend appears after the first stream windows.</span>'}</div>`;
    for (const map of Object.values(state.maps)) map?.flyTo({ center: [bus.lon,bus.lat], zoom: Math.max(13,map.getZoom()), duration: 500 });
  } catch (error) {
    $('#bus-title').textContent = vehicleId;
    $('#bus-detail').innerHTML = `<p class="empty-state">${escapeHtml(error.message)}</p>`;
  }
}

function escapeHtml(value) { return String(value ?? '').replace(/[&<>"']/g, (c) => ({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[c])); }

async function loadFleet({ silent = false } = {}) {
  try {
    const response = await api(`/api/admin/fleet?region_id=${encodeURIComponent(state.selectedRegion)}`);
    state.adminRoutes = response.routes;
    $('#fleet-limit').textContent = response.max_buses;
    $('#fleet-rows').innerHTML = response.routes.map((route) => {
      const current = Number(route.live_buses || 0);
      const minimum = Number(route.minimum_buses || response.minimum_buses_per_route || 1);
      const startCount = Math.max(minimum, current || Number(route.recommended_buses || minimum));
      return `<tr data-route-row="${escapeHtml(route.route_id)}" data-search="${escapeHtml(`${route.short_name} ${route.long_name} ${route.route_id}`.toLowerCase())}">
        <td>${escapeHtml(route.short_name || route.route_id)}<small>${escapeHtml(route.long_name || `Route ID ${route.route_id}`)}</small></td>
        <td><span class="badge ${route.minimum_buses > 1 ? 'medium' : 'low'}">${route.minimum_buses > 1 ? 'Isolated · 2 min' : `${(Number(route.length_m)/1000).toFixed(0)} km`}</span></td>
        <td>${current}</td><td><input class="fleet-count" type="number" min="${minimum}" max="${response.max_buses}" value="${startCount}" aria-label="Buses on route ${escapeHtml(route.route_id)}"></td></tr>`;
    }).join('');
    updateFleetTotal();
    if (!silent) $('#control-feedback').textContent = `${response.region_name}: ${response.routes.length.toLocaleString()} routes · recommended fleet ${Number(response.planned_total_buses).toLocaleString()} buses.`;
  } catch (error) { if (!silent) $('#control-feedback').textContent = error.message; }
}

function updateFleetTotal() {
  const total = $$('.fleet-count').reduce((sum, input) => sum + Math.max(0, Number(input.value || 0)), 0);
  $('#fleet-total').textContent = total;
  $('#apply-fleet').disabled = total < 1 || total > Number($('#fleet-limit').textContent || 200);
}

async function sendCommand(command) {
  $('#control-feedback').textContent = 'Sending command to simulator…';
  try {
    const result = await api('/api/admin/simulation/command', { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify(command) });
    $('#control-feedback').textContent = `${result.message} · ${result.command_id.slice(0,8)}`;
    setTimeout(loadHealth, 900);
    setTimeout(() => { if (state.activeView === 'admin') connectVehicleStream(); }, 1300);
    if (state.activeView === 'admin' && ['start', 'reset', 'set_fleet', 'rush_hour', 'normal_hours', 'moderate_hours'].includes(command.action)) {
      setTimeout(() => { if (state.activeView === 'admin') loadFleet({ silent: true }); }, 5000);
      setTimeout(() => { if (state.activeView === 'admin') loadFleet({ silent: true }); }, 12000);
    }
  } catch (error) { $('#control-feedback').textContent = error.message; }
}

async function loadAlerts() {
  try {
    const result = await api(`/api/alerts?active_only=true&region_id=${encodeURIComponent(state.selectedRegion || state.regionId)}`);
    $('#alert-count').textContent = `${result.alerts.length} open`;
    $('#admin-alerts').innerHTML = result.alerts.length ? result.alerts.map((alert) => `<div class="alert-row"><b>${escapeHtml(alert.alert_type)} · route ${escapeHtml(alert.route_id)}</b><small>${escapeHtml(alert.vehicle_id || 'Route')} · ${pct(alert.occ_pct || alert.pred_pct)} · ${new Date(alert.detected_at).toLocaleTimeString()}</small><button class="alert-ack" data-ack-alert="${escapeHtml(alert.event_id)}">Resolve / dismiss</button></div>`).join('') : '<p class="empty-state">No active alerts. Solved alerts are cleared from this view.</p>';
  } catch { /* API may be starting */ }
}

async function loadAdvisories() {
  if (state.user?.role !== 'admin' || state.advisoriesLoading) return;
  state.advisoriesLoading = true;
  try {
    const result = await api('/api/admin/advisories');
    $('#admin-advisories').innerHTML = result.advisories.length ? result.advisories.map((item) => `
      <div class="alert-row"><button class="advisory-route-button" data-show-advisory-route="${escapeHtml(item.route_id)}"><b>Route ${escapeHtml(item.route_id)} · Show on map</b></button><small>${escapeHtml(item.action)} · ${escapeHtml(item.rationale)} · evidence ${escapeHtml((item.evidence_ids || []).join(', ').slice(0, 16))}</small>
      <div class="advisory-actions"><button class="ghost-button" data-review="approved" data-advisory="${escapeHtml(item.advisory_id)}">Approve extra bus</button><button class="ghost-button" data-review="rejected" data-advisory="${escapeHtml(item.advisory_id)}">Dismiss</button></div></div>`).join('') : '<p class="empty-state">No pending recommendations. Alerts are generated from simulated stream windows.</p>';
    $$('[data-show-advisory-route]').forEach((button) => button.addEventListener('click', () => {
      showAdvisoryRoute(button.dataset.showAdvisoryRoute, state.selectedRegion)
        .catch((error) => { $('#control-feedback').textContent = error.message; });
    }));
    $$('[data-review]').forEach((button) => button.addEventListener('click', async () => {
      button.disabled = true;
      try {
        const reviewed = await api(`/api/admin/advisories/${encodeURIComponent(button.dataset.advisory)}/review`, {
          method: 'POST', headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({ status: button.dataset.review }),
        });
        if (reviewed.already_processed) {
          $('#control-feedback').textContent = reviewed.message;
          await loadExtraBuses();
          return;
        }
        if (reviewed.status === 'approved') {
          if (reviewed.region_id && reviewed.region_id !== state.selectedRegion) {
            state.selectedRegion = reviewed.region_id;
            $('#simulation-region').value = reviewed.region_id;
            state.regionSelectionManual = true;
            loadAlerts();
            const region = state.regions.find((item) => item.id === reviewed.region_id);
            const map = state.maps['admin-map'];
            if (map?.getSource('region-area')) map.getSource('region-area').setData(regionGeoJSON(reviewed.region_id));
            if (region) map?.flyTo({ center: region.center, zoom: region.zoom });
          }
          await showAdvisoryRoute(reviewed.route_id, reviewed.region_id || state.selectedRegion);
          $('#control-feedback').textContent = `${reviewed.message} Its live location will appear under Approved extra buses.`;
          connectVehicleStream();
          loadFleet();
          setTimeout(loadExtraBuses, 1200);
        }
        await loadAdvisories();
      } catch (error) { $('#control-feedback').textContent = error.message; }
    }));
  } catch { /* administrator data may be starting */ }
  finally { state.advisoriesLoading = false; }
}

async function loadExtraBuses() {
  if (state.user?.role !== 'admin') return;
  try {
    const result = await api(`/api/admin/advisory-buses?region_id=${encodeURIComponent(state.selectedRegion || state.regionId)}`);
    state.extraBuses = result.buses || [];
    renderAdminExtraBuses();
  } catch { /* administrator data may be starting */ }
}

function renderAdminExtraBuses() {
  const host = $('#admin-extra-buses');
  if (!host) return;
  $('#extra-bus-count').textContent = `${state.extraBuses.length} tracked`;
  host.innerHTML = state.extraBuses.length ? state.extraBuses.map((bus) => {
    const current = state.buses.find((item) => item.vehicle_id === bus.vehicle_id) || bus;
    const moving = Number(current.speed_mps || 0) > 0.5;
    const tracked = state.trackedExtraBusId === bus.vehicle_id;
    return `<div class="extra-bus-card${tracked ? ' tracked' : ''}"><span><b>${escapeHtml(bus.vehicle_id)} · ${escapeHtml(bus.short_name || bus.route_id)}</b><small>${moving ? 'Moving' : 'Stopped'} · ${crowd(current.occ_est_pct)} ${pct(current.occ_est_pct)} · ${Number(current.lat).toFixed(4)}, ${Number(current.lon).toFixed(4)}</small></span><button data-track-extra="${escapeHtml(bus.vehicle_id)}">${tracked ? 'Tracking' : 'Track'}</button></div>`;
  }).join('') : '<p class="empty-state">No approved extra bus has a recent stream snapshot yet. After approval, its first Kafka update will appear here.</p>';
  if (state.trackedExtraBusId) {
    const bus = state.buses.find((item) => item.vehicle_id === state.trackedExtraBusId)
      || state.extraBuses.find((item) => item.vehicle_id === state.trackedExtraBusId);
    $('#extra-bus-track-status').textContent = bus
      ? `Tracking ${bus.vehicle_id} on route ${bus.short_name || bus.route_id} · ${crowd(bus.occ_est_pct)} ${pct(bus.occ_est_pct)} · updated ${new Date(bus.updated_at || bus.event_ts).toLocaleTimeString()}.`
      : `Waiting for a fresh stream update for ${state.trackedExtraBusId}.`;
  }
}

async function trackExtraBus(vehicleId) {
  let bus = state.extraBuses.find((item) => item.vehicle_id === vehicleId);
  if (!bus) {
    await loadExtraBuses();
    bus = state.extraBuses.find((item) => item.vehicle_id === vehicleId);
  }
  if (!bus) {
    $('#extra-bus-track-status').textContent = 'This extra bus has no current snapshot. Wait for a simulator update and try again.';
    return;
  }
  state.trackedExtraBusId = vehicleId;
  if (bus.region_id && bus.region_id !== state.selectedRegion) {
    state.selectedRegion = bus.region_id;
    state.regionSelectionManual = true;
    $('#simulation-region').value = bus.region_id;
    state.maps['admin-map']?.getSource('region-area')?.setData(regionGeoJSON(bus.region_id));
    loadFleet(); loadAlerts();
  }
  try {
    await showAdvisoryRoute(bus.route_id, bus.region_id || state.selectedRegion);
    const point = state.buses.find((item) => item.vehicle_id === vehicleId) || bus;
    state.maps['admin-map']?.flyTo({ center: [Number(point.lon), Number(point.lat)], zoom: 14.5, duration: 700 });
    $('#extra-bus-track-status').textContent = `Tracking ${vehicleId} on ${bus.short_name || bus.route_id}. The map marker and status refresh from the live stream.`;
    connectVehicleStream();
    renderAdminExtraBuses();
  } catch (error) { $('#extra-bus-track-status').textContent = error.message; }
}

async function showAdvisoryRoute(routeId, regionId = state.selectedRegion) {
  const geo = await api(`/api/network/geojson?usable_only=true&simulated_only=true&region_id=${encodeURIComponent(regionId)}&route_id=${encodeURIComponent(routeId)}`);
  if (!geo.features.length) throw new Error('The advisory route has no usable map geometry in this region.');
  const map = state.maps['admin-map'];
  cacheRouteGeometry(geo);
  const renderAdvisoryRoute = () => {
    if (!map?.getSource('route-lines')) return false;
    map.getSource('route-lines').setData(geo);
    fitMapToRouteGeo(map, geo);
    return true;
  };
  if (!renderAdvisoryRoute() && map) map.once('idle', renderAdvisoryRoute);
  $('#control-feedback').textContent = `Showing advisory route ${routeId} in ${state.regions.find((region) => region.id === regionId)?.name || regionId}.`;
}

async function loadDq() {
  if (state.user?.role !== 'admin') return;
  try {
    const result = await api('/api/admin/dq');
    const metrics = result.metrics;
    $('#dq-malformed').textContent = metrics ? Number(metrics.malformed).toLocaleString() : '—';
    $('#dq-duplicates').textContent = metrics ? Number(metrics.duplicates).toLocaleString() : '—';
    $('#dq-late').textContent = metrics ? Number(metrics.late_dropped).toLocaleString() : '—';
  } catch { /* administrator token or stream may be starting */ }
}

function setupInteractions() {
  $$('.role-button').forEach((button) => button.addEventListener('click', () => {
    state.mapPickTarget = null;
    state.activeView = button.dataset.view;
    $$('.role-button').forEach((item) => item.classList.toggle('selected', item === button));
    $('#commuter-view').hidden = button.dataset.view !== 'commuter';
    $('#admin-view').hidden = button.dataset.view !== 'admin';
    for (const map of Object.values(state.maps)) setTimeout(() => map?.resize(), 50);
    if (button.dataset.view === 'admin' && state.user?.role === 'admin') { loadFleet(); loadAdvisories(); loadDq(); connectVehicleStream(); }
    else if (button.dataset.view === 'commuter') connectVehicleStream();
  }));
  $('#search-button').addEventListener('click', searchRoutes);
  $('#route-search').addEventListener('keydown', (event) => { if (event.key === 'Enter') searchRoutes(); });
  $('#choose-start').addEventListener('click', () => openStopPicker('start'));
  $('#choose-destination').addEventListener('click', () => openStopPicker('destination'));
  $('#change-trip').addEventListener('click', resetJourney);
  $$('[data-map-pick]').forEach((button) => button.addEventListener('click', () => setMapPickMode(button.dataset.mapPick)));
  $('#close-stop-picker').addEventListener('click', () => { $('#stop-picker').hidden = true; });
  $('#stop-search').addEventListener('input', searchStops);
  $('#stop-search').addEventListener('keydown', (event) => { if (event.key === 'Enter') { event.preventDefault(); searchStops(); } });
  $('#find-journey').addEventListener('click', findJourneyRoutes);
  $('#commuter-run-demo').addEventListener('click', () => startCommuterRouteDemo(state.selectedRouteId));
  $$('[data-commuter-profile]').forEach((button) => button.addEventListener('click', () => {
    state.commuterDemandProfile = button.dataset.commuterProfile;
    $$('[data-commuter-profile]').forEach((item) => item.classList.toggle('active', item === button));
  }));
  ['start-coordinates', 'destination-coordinates'].forEach((id) => $(`#${id}`).addEventListener('keydown', (event) => {
    if (event.key === 'Enter') { event.preventDefault(); findJourneyRoutes(); }
  }));
  $$('[data-sort]').forEach((button) => button.addEventListener('click', () => {
    state.journeySort = button.dataset.sort;
    $$('[data-sort]').forEach((item) => item.classList.toggle('selected', item === button));
    if ((state.startStop || $('#start-coordinates').value.trim())
        && (state.destinationStop || $('#destination-coordinates').value.trim())) findJourneyRoutes();
  }));
  $('#stop-results').addEventListener('click', (event) => {
    const button = event.target.closest('[data-stop-id]');
    if (button) chooseStop(button.dataset.stopId, button.dataset.stopName);
  });
  $('#route-results').addEventListener('click', (event) => {
    const routeButton = event.target.closest('[data-route]');
    const journeyButton = event.target.closest('[data-journey-option]');
    const demoLegButton = event.target.closest('[data-demo-leg]');
    const busButton = event.target.closest('[data-bus]');
    if (demoLegButton) {
      const routeId = String(demoLegButton.dataset.demoRoute);
      state.selectedRouteId = routeId;
      state.selectedRouteIds = [routeId];
      state.selectedRoute = { route_id: routeId, short_name: demoLegButton.dataset.demoShort,
        long_name: demoLegButton.dataset.demoLong, simulated: true };
      state.selectedBusId = null;
      $$('#route-results [data-demo-leg]').forEach((button) => { button.textContent = button === demoLegButton ? 'Selected demo leg' : 'Simulate this leg'; });
      syncCommuterDemoPanel();
      connectVehicleStream();
      updateSelectedRouteBusCards();
      $('#commuter-demo-feedback').textContent = `Route ${state.selectedRoute.short_name} selected for the demo. Choose the fleet size and demand, then run it.`;
    }
    else if (journeyButton) showJourneyOption(state.journeyOptions[Number(journeyButton.dataset.journeyOption)]);
    else if (routeButton) showRoute(routeButton.dataset.route);
    else if (busButton) selectBus(busButton.dataset.bus);
  });
  $('#load-fleet').addEventListener('click', loadFleet);
  $('#fleet-rows').addEventListener('input', (event) => { if (event.target.matches('.fleet-count')) updateFleetTotal(); });
  $('#fleet-filter').addEventListener('input', () => {
    const term = $('#fleet-filter').value.trim().toLowerCase();
    $$('[data-route-row]').forEach((row) => { row.hidden = term && !row.dataset.search.includes(term); });
  });
  $('#apply-fleet').addEventListener('click', () => {
    const route_bus_counts = {};
    $$('[data-route-row]').forEach((row) => {
      const input = row.querySelector('.fleet-count');
      const count = Math.max(Number(input.min || 1), Number(input.value || input.min || 1));
      route_bus_counts[row.dataset.routeRow] = count;
    });
    sendCommand({ action: 'set_fleet', region_id: state.selectedRegion, route_bus_counts });
  });
  $$('[data-action]').forEach((button) => button.addEventListener('click', () => {
    const action = button.dataset.action;
    sendCommand({ action, region_id: state.selectedRegion,
      parameters: action === 'rush_hour' ? { enabled: true, demo: true } : {} });
  }));
  $$('.speed-button').forEach((button) => button.addEventListener('click', () => {
    $$('.speed-button').forEach((item) => item.classList.toggle('active', item === button));
    sendCommand({ action: 'set_speed', speedup: Number(button.dataset.speed) });
  }));
  $('#admin-fit-map').addEventListener('click', () => {
    const region = state.regions.find((item) => item.id === state.selectedRegion);
    state.maps['admin-map']?.flyTo({ center: region?.center || [77.5946,12.9716], zoom: region?.zoom || 10.2 });
  });
  $('#simulation-region').addEventListener('change', async (event) => {
    const keepRunning = state.simulationRunning;
    state.selectedRegion = event.target.value;
    state.regionSelectionManual = true;
    loadFleet();
    loadAlerts();
    connectVehicleStream();
    const region = state.regions.find((item) => item.id === state.selectedRegion);
    const map = state.maps['admin-map'];
    if (map?.getSource('region-area')) map.getSource('region-area').setData(regionGeoJSON(state.selectedRegion));
    try {
      const geo = await api(`/api/network/geojson?usable_only=true&simulated_only=true&region_id=${encodeURIComponent(state.selectedRegion)}`);
      cacheRouteGeometry(geo);
      map?.getSource('route-lines')?.setData(geo);
      map?.flyTo({ center: region?.center || [77.5946,12.9716], zoom: region?.zoom || 10.2, duration: 450 });
      if (keepRunning) {
        $('#control-feedback').textContent = `Switching the live fleet to ${region?.name || state.selectedRegion}…`;
        sendCommand({ action: 'start', region_id: state.selectedRegion, parameters: {} });
      } else {
        $('#control-feedback').textContent = geo.features?.length
          ? `${region?.name || state.selectedRegion}: ${geo.features.length} route directions loaded. Press Start to run this region.`
          : `${region?.name || state.selectedRegion}: no simulated route geometry returned.`;
      }
    } catch (error) { $('#control-feedback').textContent = error.message; }
  });
  $('#admin-alerts').addEventListener('click', async (event) => {
    const button = event.target.closest('[data-ack-alert]');
    if (!button) return;
    button.disabled = true;
    try { await api(`/api/alerts/${encodeURIComponent(button.dataset.ackAlert)}/ack`, { method: 'POST' }); await loadAlerts(); }
    catch (error) { $('#control-feedback').textContent = error.message; }
  });
  $('#admin-extra-buses').addEventListener('click', (event) => {
    const button = event.target.closest('[data-track-extra]');
    if (button) trackExtraBus(button.dataset.trackExtra);
  });
  $$('.assistant-prompts [data-ask]').forEach((button) => button.addEventListener('click', () => {
    $('#chat-input').value = button.dataset.ask;
    $('#chat-form').requestSubmit();
  }));
  $('#chat-form').addEventListener('submit', async (event) => {
    event.preventDefault();
    const input = $('#chat-input');
    const message = input.value.trim();
    if (!message) return;
    input.value = '';
    $('#chat-messages').insertAdjacentHTML('beforeend', `<p class="user-message">${escapeHtml(message)}</p><p class="assistant-message">Looking up current route and occupancy data…</p>`);
    const responseNode = $('#chat-messages').lastElementChild;
    try {
      const result = await api('/api/assistant/chat', { method: 'POST', headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ question: message, selected_route_id: state.selectedRouteId,
          from_stop_id: state.startStop?.stop_id || null, to_stop_id: state.destinationStop?.stop_id || null }) });
      responseNode.textContent = `${result.answer} Evidence gathered with: ${result.tools.join(', ')}.`;
    } catch (error) { responseNode.textContent = error.message; }
  });
}

async function startDashboard() {
  try {
    const session = await api('/api/auth/me');
    state.user = session.user;
    $('#signed-in-user').textContent = state.user?.username || '';
    const adminButton = $('[data-view="admin"]');
    adminButton.hidden = state.user?.role !== 'admin';
    if (state.user?.role === 'admin') {
      state.activeView = 'admin';
      $$('.role-button').forEach((button) => button.classList.toggle('selected', button.dataset.view === 'admin'));
      $('#commuter-view').hidden = true;
      $('#admin-view').hidden = false;
    }
    setupInteractions();
    await loadHealth();
    setupMap('commuter-map');
    if (state.user?.role === 'admin') setupMap('admin-map', true);
    loadAlerts();
    if (state.user?.role === 'admin') { loadFleet(); loadDq(); loadAdvisories(); loadExtraBuses(); }
    connectVehicleStream();
    setInterval(loadHealth, 5000);
    setInterval(loadAlerts, 8000);
    if (state.user?.role === 'admin') { setInterval(loadDq, 8000); setInterval(loadAdvisories, 10000); setInterval(loadExtraBuses, 4000); }
    setInterval(() => {
      if (!document.hidden && state.buses.some((bus) => Number(bus.speed_mps) > 0.5)) renderBusPositionLayers();
    }, 250);
  } catch (error) {
    if (error.message) location.assign('/');
  }
}

startDashboard();
