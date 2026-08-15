import Chart from 'chart.js/auto';

// State
let eventsCount = 0;
let events = [];
let startTime = Date.now();
const MAX_EVENTS_ON_SCREEN = 50;

// Chart Instances
let clusterChart;
let sentimentChart;
let sourceChart;
let velocityChart;
let sentimentLineChart;

// Data tracking for charts
const clusterData = {};
const sentimentData = { positive: 0, neutral: 0, negative: 0 };
const sourceData = { news: 0, gdacs: 0, financial: 0, wikipedia: 0 };
const velocityData = Array(60).fill(0); // Track last 60 seconds of event velocity
const sentimentTimelineData = Array(60).fill(0); // Sentiment score over time
let currentSecond = Math.floor(Date.now() / 1000);
const entityCounts = {};
const locationStats = {}; // Tracks { count: 0, anomalies: 0 } for hotspots

// Phase 3 State
let entityNodes = new vis.DataSet([]);
let entityEdges = new vis.DataSet([]);
let networkGraph;

// DOM Elements
const feedContainer = document.getElementById('event-feed');
const alertsFeed = document.getElementById('alerts-feed');
const alertsEmptyState = document.getElementById('alerts-empty-state');
const alertsBadge = document.getElementById('alerts-badge');
const totalEventsEl = document.getElementById('total-events');
const eventsPerMinEl = document.getElementById('events-per-minute');
const statusEl = document.getElementById('connection-status');
const pulseDot = document.querySelector('.pulse-dot');
const filterPills = document.querySelectorAll('.pill');
const searchInput = document.getElementById('search-input');
const tickerContent = document.getElementById('ticker-content');
const entityCloud = document.getElementById('entity-cloud');

// Phase 2 Elements
const diagThroughput = document.getElementById('diag-throughput');
const diagKafka = document.getElementById('diag-kafka');
const diagPing = document.getElementById('diag-ping');
const miniAlertsList = document.getElementById('mini-alerts-list');
const hotspotsTbody = document.getElementById('hotspots-tbody');


// Modal Elements
const eventModal = document.getElementById('event-modal');
const modalTitle = document.getElementById('modal-title');
const modalTags = document.getElementById('modal-tags');
const modalDesc = document.getElementById('modal-desc');
const modalEntities = document.getElementById('modal-entities');
const modalKeywords = document.getElementById('modal-keywords');
const modalSource = document.getElementById('modal-source');
const modalTime = document.getElementById('modal-time');
const modalCloseBtn = document.getElementById('modal-close-btn');

// Leaflet Map instance
let map;
let unreadAlerts = 0;

// Current Filter
let currentFilter = 'all';

// Setup Chart.js defaults for dark theme
Chart.defaults.color = '#9ca3af';
Chart.defaults.font.family = "'Outfit', sans-serif";
Chart.defaults.plugins.tooltip.backgroundColor = 'rgba(16, 24, 39, 0.9)';
Chart.defaults.plugins.tooltip.padding = 10;
Chart.defaults.plugins.tooltip.cornerRadius = 8;
Chart.defaults.plugins.legend.labels.usePointStyle = true;

function initCharts() {
  const clusterCtx = document.getElementById('clusterChart').getContext('2d');
  clusterChart = new Chart(clusterCtx, {
    type: 'bar',
    data: {
      labels: [],
      datasets: [{
        label: 'Events',
        data: [],
        backgroundColor: '#8b5cf6',
        borderRadius: 4,
      }]
    },
    options: {
      responsive: true,
      maintainAspectRatio: false,
      plugins: { legend: { display: false } },
      scales: {
        y: { display: false, beginAtZero: true },
        x: { grid: { display: false, color: 'rgba(255,255,255,0.05)' } }
      }
    }
  });

  const sentimentCtx = document.getElementById('sentimentChart').getContext('2d');
  sentimentChart = new Chart(sentimentCtx, {
    type: 'doughnut',
    data: {
      labels: ['Positive', 'Neutral', 'Negative'],
      datasets: [{
        data: [0, 0, 0],
        backgroundColor: ['#10b981', '#6b7280', '#ef4444'],
        borderWidth: 0,
        hoverOffset: 4
      }]
    },
    options: {
      responsive: true,
      maintainAspectRatio: false,
      cutout: '70%',
      plugins: {
        legend: { position: 'right' }
      }
    }
  });

  const sourceCtx = document.getElementById('sourceChart').getContext('2d');
  sourceChart = new Chart(sourceCtx, {
    type: 'doughnut',
    data: {
      labels: ['News', 'Alerts', 'Market', 'Wiki'],
      datasets: [{
        data: [0, 0, 0, 0],
        backgroundColor: ['#3b82f6', '#ef4444', '#f59e0b', '#9ca3af'],
        borderWidth: 0,
        hoverOffset: 4
      }]
    },
    options: {
      responsive: true,
      maintainAspectRatio: false,
      cutout: '70%',
      plugins: {
        legend: { position: 'right' }
      }
    }
  });

  const sentimentLineCtx = document.getElementById('sentimentLineChart').getContext('2d');
  sentimentLineChart = new Chart(sentimentLineCtx, {
    type: 'line',
    data: {
      labels: Array(60).fill(''),
      datasets: [{
        label: 'Mood Index',
        data: sentimentTimelineData,
        borderColor: '#3b82f6',
        backgroundColor: 'rgba(59, 130, 246, 0.1)',
        borderWidth: 2,
        fill: true,
        tension: 0.4,
        pointRadius: 0
      }]
    },
    options: {
      responsive: true,
      maintainAspectRatio: false,
      plugins: { legend: { display: false } },
      scales: {
        y: { display: false, min: -10, max: 10 },
        x: { display: false }
      },
      animation: { duration: 0 }
    }
  });

  const velocityCtx = document.getElementById('velocityChart').getContext('2d');
  velocityChart = new Chart(velocityCtx, {
    type: 'line',
    data: {
      labels: Array(60).fill(''),
      datasets: [{
        label: 'Events/sec',
        data: velocityData,
        borderColor: '#10b981',
        backgroundColor: 'rgba(16, 185, 129, 0.1)',
        borderWidth: 2,
        fill: true,
        tension: 0.4,
        pointRadius: 0
      }]
    },
    options: {
      responsive: true,
      maintainAspectRatio: false,
      plugins: { legend: { display: false } },
      scales: {
        y: { display: false, beginAtZero: true },
        x: { display: false }
      },
      animation: {
        duration: 0
      }
    }
  });

  // Initialize Map
  map = L.map('map').setView([20, 0], 2);
  L.tileLayer('https://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png', {
    attribution: '&copy; OpenStreetMap contributors'
  }).addTo(map);



  // Initialize Entity Network
  const networkContainer = document.getElementById('entity-network');
  if (networkContainer && typeof vis !== 'undefined') {
    networkContainer.innerHTML = ''; // clear empty state
    networkGraph = new vis.Network(networkContainer, {
      nodes: entityNodes,
      edges: entityEdges
    }, {
      nodes: {
        shape: 'dot', size: 12,
        font: { color: '#9ca3af', size: 12 },
        color: { border: '#8b5cf6', background: 'rgba(139, 92, 246, 0.2)' }
      },
      edges: { color: 'rgba(156, 163, 175, 0.2)', width: 1 },
      physics: {
        forceAtlas2Based: { gravitationalConstant: -26, centralGravity: 0.005, springLength: 230, springConstant: 0.18 },
        maxVelocity: 50, solver: 'forceAtlas2Based', timestep: 0.35
      }
    });
  }
}

function updateStats() {
  eventsCount++;
  totalEventsEl.textContent = eventsCount;
  
  const minutesPassed = (Date.now() - startTime) / 60000;
  if (minutesPassed > 0) {
    eventsPerMinEl.textContent = Math.round(eventsCount / Math.max(minutesPassed, 1));
  }

  // Update velocity chart & timeline charts
  const nowSec = Math.floor(Date.now() / 1000);
  if (nowSec > currentSecond) {
    const diff = nowSec - currentSecond;
    for(let i=0; i<Math.min(diff, 60); i++){
      velocityData.shift();
      velocityData.push(0);
      sentimentTimelineData.shift();
      sentimentTimelineData.push(sentimentTimelineData[58] || 0);
    }
    currentSecond = nowSec;
    
    // Tick updates
    if (diagThroughput) diagThroughput.textContent = `${velocityData[58] || 0} ev/s`;
    if (diagKafka) diagKafka.textContent = `${Math.floor(Math.random() * 8) + 8} ms`;
    if (diagPing) diagPing.textContent = `${Math.floor(Math.random() * 4) + 2} ms`;
  }
  velocityData[59]++;
  velocityChart.update();
}

function updateCharts(event) {
  // Update Cluster Chart
  const cluster = event.event_cluster || 'general';
  clusterData[cluster] = (clusterData[cluster] || 0) + 1;
  
  // Sort and get top 5
  const sortedClusters = Object.entries(clusterData).sort((a,b) => b[1] - a[1]).slice(0, 5);
  clusterChart.data.labels = sortedClusters.map(i => i[0].charAt(0).toUpperCase() + i[0].slice(1));
  clusterChart.data.datasets[0].data = sortedClusters.map(i => i[1]);
  clusterChart.update();

  // Update Sentiment
  const sentiment = (event.sentiment || 'neutral').toLowerCase();
  if(sentimentData[sentiment] !== undefined) {
    sentimentData[sentiment]++;
    sentimentChart.data.datasets[0].data = [
      sentimentData.positive,
      sentimentData.neutral,
      sentimentData.negative
    ];
    sentimentChart.update();
    
    // Adjust Sentiment Timeline
    if (sentiment === 'positive') sentimentTimelineData[59] += 1;
    if (sentiment === 'negative') sentimentTimelineData[59] -= 1;
    // Cap limits for visual display
    sentimentTimelineData[59] = Math.max(-10, Math.min(10, sentimentTimelineData[59]));
    sentimentLineChart.update();
  }

  // Update Source
  const source = event.source_type || 'wikipedia';
  if(sourceData[source] !== undefined) {
    sourceData[source]++;
    sourceChart.data.datasets[0].data = [
      sourceData.news,
      sourceData.gdacs,
      sourceData.financial,
      sourceData.wikipedia
    ];
    sourceChart.update();
  }

  // Update Entities Cloud & Hotspots
  try {
    const ents = JSON.parse(event.entities || '{}');
    const allPlaces = [...(ents.GPE || []), ...(ents.LOC || []), ...(ents.ORG || []), ...(ents.PERSON || [])];
    allPlaces.forEach(p => {
      entityCounts[p] = (entityCounts[p] || 0) + 1;
    });
    
    const geoPlaces = [...(ents.GPE || []), ...(ents.LOC || [])];
    geoPlaces.forEach(p => {
      if (!locationStats[p]) locationStats[p] = { count: 0, anomalies: 0 };
      locationStats[p].count++;
      if (event.is_anomaly) locationStats[p].anomalies++;
    });

    const topEntities = Object.entries(entityCounts).sort((a,b) => b[1] - a[1]).slice(0, 20);
    if(topEntities.length > 0 && entityCloud) {
      entityCloud.innerHTML = topEntities.map(e => `<span class="cloud-tag">${e[0]} <small style="opacity: 0.7;">(${e[1]})</small></span>`).join('');
    }
    
    // Render Hotspots Table
    const topLocations = Object.entries(locationStats).sort((a,b) => b[1].count - a[1].count).slice(0, 5);
    if(topLocations.length > 0 && hotspotsTbody) {
      hotspotsTbody.innerHTML = topLocations.map(loc => {
        const name = loc[0];
        const stats = loc[1];
        let threatClass = 'threat-normal';
        let threatLabel = 'NORMAL';
        if (stats.anomalies > 5) { threatClass = 'threat-extreme'; threatLabel = 'EXTREME'; }
        else if (stats.anomalies > 1) { threatClass = 'threat-elevated'; threatLabel = 'ELEVATED'; }
        
        return `<tr>
          <td>${name}</td>
          <td>${stats.count}</td>
          <td><span class="threat-level ${threatClass}">${threatLabel}</span></td>
        </tr>`;
      }).join('');
    }
  } catch(e) {}
}

function getSourceIcon(source) {
  switch(source) {
    case 'news': return 'fa-newspaper';
    case 'gdacs': return 'fa-triangle-exclamation';
    case 'financial': return 'fa-chart-line';
    case 'wikipedia': return 'fa-wikipedia-w';
    default: return 'fa-rss';
  }
}

let currentSearchTerm = '';

function renderEvent(event, isReRender = false) {
  // If empty state is there, clear it
  if (!isReRender && eventsCount === 1) {
    feedContainer.innerHTML = '';
  }

  // Check filter
  if (currentFilter !== 'all' && event.source_type !== currentFilter && event.source_type !== 'system_alert') {
    return;
  }
  
  // Check Search
  if (currentSearchTerm) {
    const textToSearch = ((event.title||'') + ' ' + (event.description||'') + ' ' + (event.entities||'')).toLowerCase();
    if (!textToSearch.includes(currentSearchTerm)) return;
  }

  const el = document.createElement('div');
  el.className = `event-card glass-panel source-${event.source_type || 'wikipedia'}`;
  
  if (event.is_anomaly) {
    el.classList.add('is-anomaly');
  }
  
  const timeStr = new Date(event.ingested_at || event.timestamp || Date.now()).toLocaleTimeString();
  const sourceIcon = getSourceIcon(event.source_type);
  const title = event.title || 'Untitled Event';
  const desc = event.description || event.comment || event.content || '';
  
  const sentimentClass = event.sentiment ? `sentiment-${event.sentiment}` : '';
  
  let tagsHtml = '';
  if (event.event_cluster) {
    tagsHtml += `<span class="tag cluster"><i class="fa-solid fa-layer-group"></i> ${event.event_cluster}</span>`;
  }
  if (event.sentiment) {
    tagsHtml += `<span class="tag ${sentimentClass}"><i class="fa-solid fa-heart"></i> ${event.sentiment}</span>`;
  }

  el.innerHTML = `
    <div class="event-header">
      <div class="event-source"><i class="fa-solid ${sourceIcon}"></i> ${event.source_type || 'system'}</div>
      <div class="event-time">${timeStr}</div>
    </div>
    <div class="event-title">${title}</div>
    ${desc ? `<div class="event-desc">${desc}</div>` : ''}
    <div class="event-footer">
      ${tagsHtml}
    </div>
  `;

  // Make card clickable
  el.addEventListener('click', () => {
    modalTitle.textContent = title;
    modalTags.innerHTML = tagsHtml;
    modalDesc.textContent = desc || 'No detailed description available.';
    
    // Parse Entities
    try {
      const ents = JSON.parse(event.entities || '{}');
      let entHtml = '';
      for (const [key, val] of Object.entries(ents)) {
        entHtml += `<div style="margin-bottom: 0.3rem;"><b>${key}:</b> ${val.join(', ')}</div>`;
      }
      modalEntities.innerHTML = entHtml || 'None detected';
    } catch (e) {
      modalEntities.innerHTML = 'None detected';
    }

    // Parse Keywords
    try {
      const kw = JSON.parse(event.keywords || '[]');
      modalKeywords.innerHTML = kw.length > 0 ? kw.map(k => `<span class="tag" style="background: var(--bg-panel); border: 1px solid var(--glass-border);">${k}</span>`).join('') : 'None';
    } catch (e) {
      modalKeywords.innerHTML = 'None';
    }

    modalSource.innerHTML = `<i class="fa-solid ${sourceIcon}"></i> ${event.source_type || 'system'}`;
    modalTime.innerHTML = `<i class="fa-solid fa-clock"></i> ${timeStr}`;
    
    eventModal.style.display = 'flex';
  });

  feedContainer.prepend(el);

  // Update Entity Network Graph
  if (!isReRender && event.entities && typeof vis !== 'undefined') {
    const ents = typeof event.entities === 'string' ? Object.values(JSON.parse(event.entities) || {}).flat() : event.entities;
    if (ents && ents.length > 0) {
      // Create a center node for the event, linked to all entities
      const eventId = 'ev_' + eventsCount;
      entityNodes.add({ id: eventId, label: '', size: event.is_anomaly ? 10 : 5, color: { background: event.is_anomaly ? '#ef4444' : '#10b981' } });
      
      ents.slice(0, 3).forEach(ent => {
        const entId = 'ent_' + ent;
        if (!entityNodes.get(entId)) {
          entityNodes.add({ id: entId, label: ent, size: 15 });
        }
        entityEdges.add({ from: eventId, to: entId });
      });

      // Keep network small
      if (entityNodes.length > 60) {
        const oldestNodes = entityNodes.getIds().slice(0, 10);
        entityNodes.remove(oldestNodes);
      }
    }
  }

  // Keep DOM clean
  while (feedContainer.children.length > MAX_EVENTS_ON_SCREEN + 1) { // +1 for empty state
    feedContainer.removeChild(feedContainer.lastChild);
  }

  // Map markers (Only plot if coordinates exist and not just rerendering)
  if (!isReRender && event.lat !== undefined && event.lng !== undefined) {
    const lat = event.lat;
    const lng = event.lng;
  
    const markerColor = event.is_anomaly ? '#ef4444' : (event.source_type === 'news' ? '#3b82f6' : '#8b5cf6');
    
    const circle = L.circleMarker([lat, lng], {
      radius: event.is_anomaly ? 8 : 4,
      fillColor: markerColor,
      color: markerColor,
      weight: 1,
      opacity: 1,
      fillOpacity: 0.6
    }).addTo(map);

    circle.bindPopup(`<b>${title}</b><br>${event.source_type}`);

    // Fade out map markers over time
    setTimeout(() => {
      map.removeLayer(circle);
    }, 120000); // 2 minutes
  }

  // Alerts & Ticker Feed logic (Only apply for new incoming events)
  if (!isReRender && (event.is_anomaly || event.source_type === 'system_alert')) {
    if (alertsEmptyState) alertsEmptyState.style.display = 'none';
    
    // Add to main alerts inbox
    const alertEl = el.cloneNode(true);
    alertEl.addEventListener('click', () => el.click());
    alertsFeed.prepend(alertEl);
    
    // Add to Mini Alerts Inbox
    if (miniAlertsList) {
      const emptyState = miniAlertsList.querySelector('.empty-state');
      if (emptyState) emptyState.remove();
      const miniAlert = document.createElement('div');
      miniAlert.className = 'mini-alert-item';
      miniAlert.textContent = `[${timeStr}] ${title}`;
      miniAlert.addEventListener('click', () => el.click());
      miniAlertsList.prepend(miniAlert);
      if (miniAlertsList.children.length > 5) miniAlertsList.removeChild(miniAlertsList.lastChild);
    }
    
    unreadAlerts++;
    alertsBadge.textContent = unreadAlerts;
    
    while (alertsFeed.children.length > MAX_EVENTS_ON_SCREEN + 1) {
      alertsFeed.removeChild(alertsFeed.lastChild);
    }
  }

  if (!isReRender && (event.is_anomaly || event.sentiment === 'NEGATIVE')) {
    const span = document.createElement('span');
    span.className = `ticker-item ${event.is_anomaly ? 'critical' : ''}`;
    span.innerHTML = `<i class="fa-solid fa-circle-exclamation"></i> [${timeStr}] ${title}`;
    if (tickerContent.querySelector('.ticker-item').textContent.includes('System initializing')) {
      tickerContent.innerHTML = '';
    }
    tickerContent.appendChild(span);
    if(tickerContent.children.length > 20) tickerContent.removeChild(tickerContent.firstChild);
  }
}

function connectWebSocket() {
  const ws = new WebSocket('ws://localhost:8000/ws');
  
  ws.onopen = () => {
    statusEl.textContent = 'Live';
    pulseDot.style.animationPlayState = 'running';
    pulseDot.style.backgroundColor = 'var(--accent-green)';
  };
  
  ws.onmessage = (msg) => {
    try {
      const event = JSON.parse(msg.data);
      events.push(event);
      if(events.length > 1000) events.shift(); // Keep memory bounded
      
      updateStats();
      updateCharts(event);
      renderEvent(event);
    } catch(e) {
      console.error('Error parsing WS message', e);
    }
  };
  
  ws.onclose = () => {
    statusEl.textContent = 'Disconnected';
    pulseDot.style.animationPlayState = 'paused';
    pulseDot.style.backgroundColor = 'var(--text-muted)';
    setTimeout(connectWebSocket, 3000); // Reconnect loop
  };
}

function reRenderFeed() {
  feedContainer.innerHTML = '';
  const filtered = events.filter(e => {
    const matchFilter = currentFilter === 'all' || e.source_type === currentFilter || e.source_type === 'system_alert';
    if (!matchFilter) return false;
    if (!currentSearchTerm) return true;
    
    const textToSearch = ((e.title||'') + ' ' + (e.description||'') + ' ' + (e.entities||'')).toLowerCase();
    return textToSearch.includes(currentSearchTerm);
  });
  
  const toRender = filtered.slice(-MAX_EVENTS_ON_SCREEN).reverse();
  
  if (toRender.length === 0) {
    feedContainer.innerHTML = `
      <div class="empty-state">
        <i class="fa-solid fa-satellite-dish"></i>
        <p>No events match your criteria.</p>
      </div>
    `;
  } else {
    toRender.forEach(e => renderEvent(e, true));
  }
}

// Setup Filters
filterPills.forEach(pill => {
  pill.addEventListener('click', (e) => {
    filterPills.forEach(p => p.classList.remove('active'));
    pill.classList.add('active');
    currentFilter = pill.dataset.filter;
    reRenderFeed();
  });
});

// Setup Search
if (searchInput) {
  searchInput.addEventListener('input', (e) => {
    currentSearchTerm = e.target.value.toLowerCase();
    reRenderFeed();
  });
}

// Setup Navigation Links
const navLinks = document.querySelectorAll('.nav-menu a');
const views = {
  'Dashboard': document.getElementById('dashboard-view'),
  'World Map': document.getElementById('world-map-view'),
  'Alerts': document.getElementById('alerts-view'),
  'Settings': document.getElementById('settings-view')
};

navLinks.forEach(link => {
  link.addEventListener('click', (e) => {
    e.preventDefault();
    
    // Update active nav styling
    navLinks.forEach(l => l.classList.remove('active'));
    link.classList.add('active');

    // Hide all views and clean up inline styles
    Object.values(views).forEach(v => {
      if (v) {
        v.classList.remove('active');
        // Remove any old inline styles that might conflict with our CSS
        v.style.display = '';
        v.style.height = '';
        v.style.flexDirection = '';
      }
    });

    // Show target view
    const text = link.textContent.trim().replace(/[0-9]+$/, '').trim(); // Remove badge numbers if any
    const targetView = views[text];
    
    if (targetView) {
      targetView.classList.add('active');
      if (text === 'World Map' && map) {
        setTimeout(() => map.invalidateSize(), 100);
      }
    }
  });
});

// Settings interactions
const slider = document.querySelector('.custom-slider');
const sliderVal = document.querySelector('.slider-val');
if(slider && sliderVal) {
  slider.addEventListener('input', (e) => {
    sliderVal.textContent = e.target.value + '%';
  });
}

// Clear Alerts
const clearAlertsBtn = document.getElementById('clear-alerts-btn');
if(clearAlertsBtn) {
  clearAlertsBtn.addEventListener('click', () => {
    const alerts = Array.from(alertsFeed.children).filter(c => c.id !== 'alerts-empty-state');
    alerts.forEach(a => a.remove());
    if(alertsEmptyState) alertsEmptyState.style.display = 'flex';
    unreadAlerts = 0;
    alertsBadge.textContent = '0';
  });
}

// Modal closing logic
if (modalCloseBtn) {
  modalCloseBtn.addEventListener('click', () => eventModal.style.display = 'none');
}
if (eventModal) {
  eventModal.addEventListener('click', (e) => {
    if (e.target === eventModal) eventModal.style.display = 'none';
  });
}

// Init
async function initApp() {
  initCharts();
  
  // Fetch recent events to populate the dashboard immediately
  try {
    const res = await fetch('http://localhost:8000/api/events?limit=50');
    if (res.ok) {
      const initialEvents = await res.json();
      // Render in reverse so the newest is at the top
      initialEvents.reverse().forEach(event => {
        events.push(event);
        updateStats();
        updateCharts(event);
        renderEvent(event);
      });
    }
  } catch(e) {
    console.error('Failed to fetch initial events:', e);
  }

  connectWebSocket();
}

initApp();



// Fullscreen Chart Logic
const chartBackdrop = document.getElementById("chart-backdrop");
document.querySelectorAll(".expand-btn").forEach(btn => {
  btn.addEventListener("click", (e) => {
    const card = e.target.closest(".chart-card");
    const isExpanded = card.classList.contains("fullscreen-chart");
    
    if (isExpanded) {
      card.classList.remove("fullscreen-chart");
      e.target.classList.remove("fa-compress");
      e.target.classList.add("fa-expand");
      chartBackdrop.style.display = "none";
    } else {
      card.classList.add("fullscreen-chart");
      e.target.classList.remove("fa-expand");
      e.target.classList.add("fa-compress");
      chartBackdrop.style.display = "block";
    }
    
    // Trigger resize to force Chart.js and vis.js to fill the new container size
    setTimeout(() => {
      window.dispatchEvent(new Event("resize"));
    }, 50);
  });
});

if (chartBackdrop) {
  chartBackdrop.addEventListener("click", () => {
    const expandedCard = document.querySelector(".fullscreen-chart");
    if (expandedCard) {
      const btn = expandedCard.querySelector(".expand-btn");
      if (btn) btn.click(); // Trigger compress logic
    }
  });
}

