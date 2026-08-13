import './style.css';

// DOM Elements
const connectionStatus = document.getElementById('connection-status');
const pulseDot = document.querySelector('.pulse-dot');
const totalEventsEl = document.getElementById('total-events');
const eventsPerMinEl = document.getElementById('events-per-minute');
const eventFeed = document.getElementById('event-feed');
const filterPills = document.querySelectorAll('.pill');

// State
let allEvents = [];
let currentFilter = 'all';
let totalEventsCount = 0;
let eventsInLastMinute = 0;
let startTime = Date.now();

// Charts
let clusterChart, sentimentChart, sourceChart;

// Initialize Charts
function initCharts() {
  Chart.defaults.color = '#94a3b8';
  Chart.defaults.font.family = "'Outfit', sans-serif";
  
  // 1. Cluster Chart (Scatter)
  const ctxCluster = document.getElementById('clusterChart').getContext('2d');
  clusterChart = new Chart(ctxCluster, {
    type: 'scatter',
    data: {
      datasets: [{
        label: 'Events by Sentiment',
        data: [],
        backgroundColor: '#06b6d4'
      }]
    },
    options: {
      responsive: true,
      maintainAspectRatio: false,
      scales: {
        x: { grid: { color: 'rgba(255,255,255,0.05)' } },
        y: { grid: { color: 'rgba(255,255,255,0.05)' } }
      },
      plugins: { legend: { display: false } }
    }
  });

  // 2. Sentiment Chart (Doughnut)
  const ctxSentiment = document.getElementById('sentimentChart').getContext('2d');
  sentimentChart = new Chart(ctxSentiment, {
    type: 'doughnut',
    data: {
      labels: ['Positive', 'Neutral', 'Negative', 'Burst (High Severity)'],
      datasets: [{
        data: [0, 0, 0, 0],
        backgroundColor: ['#10b981', '#94a3b8', '#ef4444', '#8b5cf6'],
        borderWidth: 0,
        hoverOffset: 4
      }]
    },
    options: {
      responsive: true,
      maintainAspectRatio: false,
      plugins: {
        legend: { position: 'right', labels: { boxWidth: 12 } }
      },
      cutout: '70%'
    }
  });

  // 3. Source Chart (Bar)
  const ctxSource = document.getElementById('sourceChart').getContext('2d');
  sourceChart = new Chart(ctxSource, {
    type: 'bar',
    data: {
      labels: ['News', 'Alerts', 'Financial', 'Wiki'],
      datasets: [{
        label: 'Events per Source',
        data: [0, 0, 0, 0],
        backgroundColor: 'rgba(6, 182, 212, 0.6)',
        borderColor: '#06b6d4',
        borderWidth: 1,
        borderRadius: 4
      }]
    },
    options: {
      responsive: true,
      maintainAspectRatio: false,
      scales: {
        y: { beginAtZero: true, grid: { color: 'rgba(255,255,255,0.05)' } },
        x: { grid: { display: false } }
      },
      plugins: { legend: { display: false } }
    }
  });
}

// Update Charts Logic
function updateCharts(event) {
  // Update Source
  const sourceMap = { 'news': 0, 'gdacs': 1, 'financial': 2, 'wikipedia': 3 };
  if (sourceMap[event.source] !== undefined) {
    sourceChart.data.datasets[0].data[sourceMap[event.source]]++;
    sourceChart.update();
  }

  // Update Sentiment
  if (event.is_burst) {
    sentimentChart.data.datasets[0].data[3]++;
  } else {
    if (event.sentiment_score > 0.2) sentimentChart.data.datasets[0].data[0]++;
    else if (event.sentiment_score < -0.2) sentimentChart.data.datasets[0].data[2]++;
    else sentimentChart.data.datasets[0].data[1]++;
  }
  sentimentChart.update();

  // Update Cluster (mocking x,y for visual demo based on embedding if exists, or random)
  const x = event.embeddings ? event.embeddings[0] : (Math.random() - 0.5);
  const y = event.embeddings ? event.embeddings[1] : (Math.random() - 0.5);
  clusterChart.data.datasets[0].data.push({ x: x, y: y });
  if (clusterChart.data.datasets[0].data.length > 50) {
    clusterChart.data.datasets[0].data.shift(); // keep it clean
  }
  clusterChart.update();
}

// Format Time
function formatTime(isoString) {
  const date = new Date(isoString);
  return date.toLocaleTimeString([], { hour: '2-digit', minute: '2-digit', second: '2-digit' });
}

// Render Event Card
function createEventCard(event) {
  const card = document.createElement('div');
  
  let sentimentClass = 'neutral';
  if (event.is_burst) sentimentClass = 'burst';
  else if (event.sentiment_score > 0.2) sentimentClass = 'positive';
  else if (event.sentiment_score < -0.2) sentimentClass = 'negative';

  card.className = `event-card ${sentimentClass}`;
  card.dataset.source = event.source;

  let tagsHtml = '';
  if (event.keywords) {
    try {
      const keywords = JSON.parse(event.keywords);
      tagsHtml = keywords.slice(0, 3).map(k => `<span class="tag">${k}</span>`).join('');
    } catch(e) {}
  }
  if (event.is_burst) {
    tagsHtml += `<span class="tag" style="background: rgba(139, 92, 246, 0.2); color: #c4b5fd; border-color: #8b5cf6;">⚠️ BURST</span>`;
  }

  const icons = {
    news: 'fa-newspaper',
    wikipedia: 'fa-wikipedia-w',
    financial: 'fa-chart-line',
    gdacs: 'fa-triangle-exclamation'
  };

  card.innerHTML = `
    <div class="event-header">
      <div class="event-source"><i class="fa-solid ${icons[event.source] || 'fa-bolt'}"></i> ${event.source}</div>
      <div class="event-time">${formatTime(event.ingested_at || Date.now())}</div>
    </div>
    <div class="event-text">${event.text}</div>
    <div class="event-footer">
      <div class="event-tags">${tagsHtml}</div>
      <div class="event-sentiment sentiment-${sentimentClass}">
        Score: ${(event.sentiment_score || 0).toFixed(2)}
      </div>
    </div>
  `;

  return card;
}

// Handle Incoming Events
function processEvent(event) {
  // Stats
  totalEventsCount++;
  totalEventsEl.innerText = totalEventsCount;
  
  const minutesElapsed = (Date.now() - startTime) / 60000;
  eventsPerMinEl.innerText = Math.round(totalEventsCount / Math.max(minutesElapsed, 1));

  // Store
  allEvents.unshift(event);
  if (allEvents.length > 200) allEvents.pop(); // keep memory clean

  // Filter & Render
  if (currentFilter === 'all' || currentFilter === event.source) {
    if (eventFeed.querySelector('.empty-state')) {
      eventFeed.innerHTML = ''; // clear empty state
    }
    const card = createEventCard(event);
    eventFeed.prepend(card);
    
    // Prune DOM
    if (eventFeed.children.length > 50) {
      eventFeed.removeChild(eventFeed.lastChild);
    }
  }

  // Visuals
  updateCharts(event);
}

// Setup Filters
filterPills.forEach(pill => {
  pill.addEventListener('click', (e) => {
    // UI Update
    filterPills.forEach(p => p.classList.remove('active'));
    e.target.classList.add('active');
    
    // State Update
    currentFilter = e.target.dataset.filter;
    
    // Re-render feed
    eventFeed.innerHTML = '';
    const filtered = currentFilter === 'all' 
      ? allEvents 
      : allEvents.filter(ev => ev.source === currentFilter);
      
    if (filtered.length === 0) {
      eventFeed.innerHTML = `
        <div class="empty-state">
          <i class="fa-solid fa-satellite-dish"></i>
          <p>No events match this filter.</p>
        </div>
      `;
    } else {
      filtered.slice(0, 50).forEach(ev => {
        eventFeed.appendChild(createEventCard(ev));
      });
    }
  });
});


// WebSocket Connection
function connectWebSocket() {
  const ws = new WebSocket('ws://localhost:8000/ws');
  
  ws.onopen = () => {
    connectionStatus.innerText = 'Connected - Live';
    pulseDot.className = 'pulse-dot connected';
    console.log("WebSocket Connected");
  };

  ws.onmessage = (msg) => {
    try {
      const data = JSON.parse(msg.data);
      processEvent(data);
    } catch (e) {
      console.error("Failed to parse event", e);
    }
  };

  ws.onclose = () => {
    connectionStatus.innerText = 'Disconnected - Retrying...';
    pulseDot.className = 'pulse-dot disconnected';
    console.log("WebSocket Disconnected. Retrying in 3s...");
    setTimeout(connectWebSocket, 3000);
  };
  
  ws.onerror = (err) => {
    console.error("WebSocket Error:", err);
    ws.close();
  };
}

// Startup
document.addEventListener('DOMContentLoaded', () => {
  initCharts();
  connectWebSocket();
});
