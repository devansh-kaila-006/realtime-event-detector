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

// Data tracking for charts
const clusterData = {};
const sentimentData = { positive: 0, neutral: 0, negative: 0 };
const sourceData = { news: 0, gdacs: 0, financial: 0, wikipedia: 0 };

// DOM Elements
const feedContainer = document.getElementById('event-feed');
const totalEventsEl = document.getElementById('total-events');
const eventsPerMinEl = document.getElementById('events-per-minute');
const statusEl = document.getElementById('connection-status');
const pulseDot = document.querySelector('.pulse-dot');
const filterPills = document.querySelectorAll('.pill');

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
}

function updateStats() {
  eventsCount++;
  totalEventsEl.textContent = eventsCount;
  
  const minutesPassed = (Date.now() - startTime) / 60000;
  if (minutesPassed > 0) {
    eventsPerMinEl.textContent = Math.round(eventsCount / Math.max(minutesPassed, 1));
  }
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
  const sentiment = event.sentiment || 'neutral';
  if(sentimentData[sentiment] !== undefined) {
    sentimentData[sentiment]++;
    sentimentChart.data.datasets[0].data = [
      sentimentData.positive,
      sentimentData.neutral,
      sentimentData.negative
    ];
    sentimentChart.update();
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

function renderEvent(event) {
  // If empty state is there, clear it
  if (eventsCount === 1) {
    feedContainer.innerHTML = '';
  }

  // Check filter
  if (currentFilter !== 'all' && event.source_type !== currentFilter) {
    return;
  }

  const el = document.createElement('div');
  el.className = `event-card glass-panel source-${event.source_type || 'wikipedia'}`;
  
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

  feedContainer.prepend(el);

  // Keep DOM clean
  while (feedContainer.children.length > MAX_EVENTS_ON_SCREEN) {
    feedContainer.removeChild(feedContainer.lastChild);
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

// Setup Filters
filterPills.forEach(pill => {
  pill.addEventListener('click', (e) => {
    filterPills.forEach(p => p.classList.remove('active'));
    pill.classList.add('active');
    currentFilter = pill.dataset.filter;
    
    // Rerender visible events
    feedContainer.innerHTML = '';
    const filtered = currentFilter === 'all' ? events : events.filter(e => e.source_type === currentFilter);
    const toRender = filtered.slice(-MAX_EVENTS_ON_SCREEN).reverse();
    
    if (toRender.length === 0) {
      feedContainer.innerHTML = `
        <div class="empty-state">
          <i class="fa-solid fa-satellite-dish"></i>
          <p>No events match this filter.</p>
        </div>
      `;
    } else {
      toRender.forEach(renderEvent);
    }
  });
});

// Init
initCharts();
connectWebSocket();
