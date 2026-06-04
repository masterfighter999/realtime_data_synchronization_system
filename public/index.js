// AeroSync Dashboard Client-Side Logic

const API_BASE = window.location.origin;
// WebSocket gateway runs on port 3001
const WS_BASE = `${window.location.protocol}//${window.location.hostname}:3001`;

// State variables
let jwtToken = localStorage.getItem('aerosync_token') || null;
let currentUser = null;
let socket = null;
let ordersMap = new Map(); // orderId -> order object
let lastSequence = '0';
let metricsInterval = null;

// DOM Elements
const authScreen = document.getElementById('auth-screen');
const appScreen = document.getElementById('app-screen');
const loginForm = document.getElementById('login-form');
const usernameInput = document.getElementById('login-username');
const roleSelect = document.getElementById('login-role');
const authError = document.getElementById('auth-error');
const authErrorMsg = document.getElementById('auth-error-msg');
const btnLoginAdmin = document.getElementById('btn-login-admin');
const btnLoginCustomer = document.getElementById('btn-login-customer');

const userDisplayName = document.getElementById('user-display-name');
const userDisplayRole = document.getElementById('user-display-role');
const btnLogout = document.getElementById('btn-logout');

const wsStatusBadge = document.getElementById('ws-status-badge');
const wsStatusText = document.getElementById('ws-status-text');
const socketToggle = document.getElementById('socket-connection-toggle');
const btnSyncReplay = document.getElementById('btn-sync-replay');

const metricLag = document.getElementById('metric-lag');
const metricClients = document.getElementById('metric-clients');
const metricEps = document.getElementById('metric-eps');
const breakerDb = document.getElementById('breaker-db');
const breakerCache = document.getElementById('breaker-cache');

const orderForm = document.getElementById('order-creation-form');
const orderCustomerInput = document.getElementById('order-customer');
const orderProductInput = document.getElementById('order-product');
const orderStatusSelect = document.getElementById('order-status');

const ordersTableBody = document.getElementById('orders-table-body');
const btnRefreshOrders = document.getElementById('btn-refresh-orders');

const cdcConsoleFeed = document.getElementById('cdc-console-feed');
const btnClearConsole = document.getElementById('btn-clear-console');
const backpressureAlert = document.getElementById('backpressure-alert');

// Simple JWT decoder (no verification, just reads payload)
function parseJwt(token) {
  try {
    const base64Url = token.split('.')[1];
    const base64 = base64Url.replace(/-/g, '+').replace(/_/g, '/');
    const jsonPayload = decodeURIComponent(window.atob(base64).split('').map(function(c) {
        return '%' + ('00' + c.charCodeAt(0).toString(16)).slice(-2);
    }).join(''));
    return JSON.parse(jsonPayload);
  } catch (e) {
    return null;
  }
}

// Check initial session
function initSession() {
  if (jwtToken) {
    currentUser = parseJwt(jwtToken);
    if (currentUser && currentUser.exp * 1000 > Date.now()) {
      showApp();
    } else {
      logout();
    }
  } else {
    showAuth();
  }
}

function showAuth() {
  authScreen.classList.remove('hidden');
  appScreen.classList.add('hidden');
  stopPoller();
  disconnectSocket();
}

function showApp() {
  authScreen.classList.add('hidden');
  appScreen.classList.remove('hidden');

  userDisplayName.textContent = currentUser.username;
  userDisplayRole.textContent = currentUser.role;
  userDisplayRole.className = `badge ${currentUser.role === 'admin' ? 'btn-primary' : 'btn-secondary'}`;

  // Configure order form defaults based on role
  if (currentUser.role === 'admin') {
    orderCustomerInput.value = '';
    orderCustomerInput.disabled = false;
    orderCustomerInput.placeholder = 'e.g. customer_a';
  } else {
    orderCustomerInput.value = currentUser.username;
    orderCustomerInput.disabled = true;
  }

  // Load initial orders snapshot
  fetchOrdersSnapshot();

  // Establish live Socket.IO connection
  connectSocket();

  // Start metrics poller
  startPoller();
}

// REST API helper calls
async function login(username, role) {
  try {
    const res = await fetch(`${API_BASE}/auth/login`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ username, role }),
    });

    if (!res.ok) {
      const err = await res.json();
      throw new Error(err.error || 'Authentication failed');
    }

    const data = await res.json();
    jwtToken = data.token;
    localStorage.setItem('aerosync_token', jwtToken);
    currentUser = parseJwt(jwtToken);
    authError.classList.add('hidden');
    showApp();
  } catch (error) {
    authErrorMsg.textContent = error.message;
    authError.classList.remove('hidden');
  }
}

function logout() {
  localStorage.removeItem('aerosync_token');
  jwtToken = null;
  currentUser = null;
  showAuth();
}

// Fetch Complete Snapshot (orders list + lastSequence tracking)
async function fetchOrdersSnapshot() {
  try {
    logToConsole('system', 'Requesting database snapshot...');
    const res = await fetch(`${API_BASE}/orders/snapshot`, {
      headers: { 'Authorization': `Bearer ${jwtToken}` },
    });

    if (!res.ok) {
      if (res.status === 401 || res.status === 403) logout();
      throw new Error('Failed to retrieve order snapshot');
    }

    const data = await res.json();
    
    // Clear old map
    ordersMap.clear();

    // Load orders
    if (data.orders) {
      data.orders.forEach(order => {
        ordersMap.set(order.id, order);
      });
    }

    // Set highwater mark sequence number
    lastSequence = data.lastSequence || '0';

    logToConsole('system', `Snapshot synchronized. Tracked highwater offset sequence: ${lastSequence}`, data);
    renderOrdersTable();
  } catch (error) {
    logToConsole('system', `CRITICAL: Snapshot fetch failed: ${error.message}`);
  }
}

// Replay/Sync offline event updates
async function syncOfflineState() {
  if (!jwtToken) return;
  try {
    logToConsole('system', `Initiating outbox replay catchup starting from sequence: ${lastSequence}`);
    
    const res = await fetch(`${API_BASE}/events/replay?from=${lastSequence}`, {
      headers: { 'Authorization': `Bearer ${jwtToken}` },
    });

    if (!res.ok) throw new Error('Replay query failed');

    const events = await res.json();
    logToConsole('system', `Replay stream returned ${events.length} missed events.`);

    if (events.length > 0) {
      // Process events sequentially to reconcile state
      events.forEach(event => {
        applyEventToState(event, true);
      });
      renderOrdersTable();
    } else {
      logToConsole('system', 'Client local state is already fully synchronized.');
    }
  } catch (error) {
    logToConsole('system', `Replay reconciliation failed: ${error.message}. Performing complete snapshot reset.`);
    fetchOrdersSnapshot();
  }
}

// Socket.IO Handlers
function connectSocket() {
  if (socket) return;

  logToConsole('system', 'Establishing secure WebSocket handshake...');
  socketStatusChange(false, 'Connecting...');

  socket = io(WS_BASE, {
    query: { token: jwtToken },
    transports: ['websocket'],
    reconnection: true,
  });

  socket.on('connect', () => {
    logToConsole('system', `WebSocket connection established. Socket ID: ${socket.id}`);
    socketStatusChange(true, 'Connected');
    socketToggle.checked = true;
    
    // Auto-sync offline updates upon connection/reconnection
    syncOfflineState();
  });

  socket.on('disconnect', (reason) => {
    logToConsole('system', `WebSocket disconnected. Reason: ${reason}`);
    socketStatusChange(false, 'Disconnected');
    socketToggle.checked = false;
  });

  socket.on('connect_error', (err) => {
    logToConsole('system', `WebSocket Handshake Rejected: ${err.message}`);
    socketStatusChange(false, 'Error');
    socketToggle.checked = false;
  });

  // Main event consumer (receives CDC Outbox payloads)
  socket.on('order_event', (event, ack) => {
    // Process event
    applyEventToState(event, false);

    // Call callback immediately to acknowledge event (Flow control / Backpressure mitigation)
    if (ack) {
      ack();
    }
  });

  socket.on('warning', (warning) => {
    logToConsole('warning', `[GATEWAY WARNING] ${warning.message}`);
    if (warning.type === 'CLIENT_BACKPRESSURE') {
      backpressureAlert.classList.remove('hidden');
    }
  });
}

function disconnectSocket() {
  if (socket) {
    socket.disconnect();
    socket = null;
  }
  socketStatusChange(false, 'Disconnected');
  socketToggle.checked = false;
}

function socketStatusChange(connected, text) {
  if (connected) {
    wsStatusBadge.className = 'status-badge status-connected';
  } else {
    wsStatusBadge.className = 'status-badge status-disconnected';
  }
  wsStatusText.textContent = text;
}

// Event Application Logic
function applyEventToState(event, isReplaying = false) {
  const eventSeq = BigInt(event.sequenceNumber || event.sequence_number || '0');
  const currentSeq = BigInt(lastSequence);

  // Ignore older/duplicate events to maintain idempotency
  if (eventSeq <= currentSeq && !isReplaying) {
    return;
  }

  // Update tracking sequence number
  if (eventSeq > currentSeq) {
    lastSequence = eventSeq.toString();
  }

  const orderPayload = event.payload;
  const orderId = orderPayload.id;

  // Log in live console
  const eventType = event.eventType || event.event_type;
  logToConsole(eventType, `Event ${eventType} // Sequence #${eventSeq} // ID ${orderId}`, event);

  // Clear backpressure warning banner if we get a new active synchronized event and it's resolved
  backpressureAlert.classList.add('hidden');

  let flashClass = '';
  if (eventType === 'ORDER_CREATED') {
    ordersMap.set(orderId, orderPayload);
    flashClass = 'flash-create';
  } else if (eventType === 'ORDER_UPDATED') {
    ordersMap.set(orderId, orderPayload);
    flashClass = 'flash-update';
  } else if (eventType === 'ORDER_DELETED') {
    ordersMap.delete(orderId);
    // Visual flash deletion handled by tagging target row beforehand or simple redraw
  }

  // If live event (not in batch replay), re-render table and trigger glow animation
  if (!isReplaying) {
    renderOrdersTable(orderId, flashClass);
  }
}

// Render Table
function renderOrdersTable(glowId = null, glowClass = '') {
  if (ordersMap.size === 0) {
    ordersTableBody.innerHTML = `
      <tr class="placeholder-row">
        <td colspan="6">
          <div class="empty-state">
            <i class="fa-solid fa-folder-open"></i>
            <p>No active orders recorded in database.</p>
          </div>
        </td>
      </tr>
    `;
    return;
  }

  // Sort orders by ID ascending
  const sortedOrders = Array.from(ordersMap.values()).sort((a, b) => a.id - b.id);
  
  ordersTableBody.innerHTML = '';
  
  sortedOrders.forEach(order => {
    const tr = document.createElement('tr');
    tr.id = `order-row-${order.id}`;

    // Apply flash animations
    if (glowId === order.id && glowClass) {
      tr.className = glowClass;
    }

    const updatedAtStr = new Date(order.updated_at || order.updatedAt).toLocaleTimeString();

    // Determine status badge class
    let badgeClass = 'pending';
    if (order.status === 'shipped') badgeClass = 'shipped';
    if (order.status === 'delivered') badgeClass = 'delivered';

    // Actions depending on user role
    const isOwner = currentUser.role === 'admin' || currentUser.username === order.customer_name;
    const actionButtons = isOwner ? `
      <div class="action-buttons">
        <button class="btn-table-action" onclick="cycleOrderStatus(${order.id}, '${order.status}')" title="Cycle Status">
          <i class="fa-solid fa-rotate"></i>
        </button>
        ${currentUser.role === 'admin' ? `
          <button class="btn-table-action btn-table-delete" onclick="deleteOrder(${order.id})" title="Delete Order (Admin Only)">
            <i class="fa-solid fa-trash-can"></i>
          </button>
        ` : ''}
      </div>
    ` : '<span class="text-muted" style="font-size:0.75rem;">Restricted</span>';

    tr.innerHTML = `
      <td><strong>#${order.id}</strong></td>
      <td>${order.product_name || order.productName}</td>
      <td><code>${order.customer_name || order.customerName}</code></td>
      <td><span class="badge-status ${badgeClass}">${order.status}</span></td>
      <td><span class="text-muted">${updatedAtStr}</span></td>
      <td>${actionButtons}</td>
    `;

    ordersTableBody.appendChild(tr);
  });
}

// REST requests for CRUD actions
async function createOrder(customer_name, product_name, status) {
  try {
    const res = await fetch(`${API_BASE}/orders`, {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
        'Authorization': `Bearer ${jwtToken}`,
      },
      body: JSON.stringify({ customer_name, product_name, status }),
    });

    if (!res.ok) {
      const err = await res.json();
      throw new Error(err.error || 'Failed to create order');
    }

    const newOrder = await res.json();
    logToConsole('system', `Order #${newOrder.id} successfully created via REST API`);
    orderProductInput.value = '';
  } catch (error) {
    logToConsole('system', `CRUD Error: ${error.message}`);
  }
}

window.cycleOrderStatus = async function(orderId, currentStatus) {
  let nextStatus = 'pending';
  if (currentStatus === 'pending') nextStatus = 'shipped';
  else if (currentStatus === 'shipped') nextStatus = 'delivered';

  try {
    const res = await fetch(`${API_BASE}/orders/${orderId}`, {
      method: 'PATCH',
      headers: {
        'Content-Type': 'application/json',
        'Authorization': `Bearer ${jwtToken}`,
      },
      body: JSON.stringify({ status: nextStatus }),
    });

    if (!res.ok) {
      const err = await res.json();
      throw new Error(err.error || 'Failed to update order status');
    }

    logToConsole('system', `PATCH status change requested for Order #${orderId} -> ${nextStatus}`);
  } catch (error) {
    logToConsole('system', `CRUD Error: ${error.message}`);
  }
};

window.deleteOrder = async function(orderId) {
  if (!confirm(`Are you sure you want to delete order #${orderId}?`)) return;

  try {
    // Add deletion flash animation first on client
    const tr = document.getElementById(`order-row-${orderId}`);
    if (tr) {
      tr.className = 'flash-delete';
    }

    const res = await fetch(`${API_BASE}/orders/${orderId}`, {
      method: 'DELETE',
      headers: { 'Authorization': `Bearer ${jwtToken}` },
    });

    if (!res.ok) {
      const err = await res.json();
      throw new Error(err.error || 'Failed to delete order');
    }

    logToConsole('system', `DELETE request submitted for Order #${orderId}`);
  } catch (error) {
    logToConsole('system', `CRUD Error: ${error.message}`);
    fetchOrdersSnapshot(); // reload snapshot to clear deleted animation state on failure
  }
};

// Console Log UI helper
function logToConsole(type, msg, data = null) {
  const line = document.createElement('div');
  let typeClass = 'system-line';
  
  if (type === 'ORDER_CREATED') typeClass = 'event-create-line';
  else if (type === 'ORDER_UPDATED') typeClass = 'event-update-line';
  else if (type === 'ORDER_DELETED') typeClass = 'event-delete-line';
  else if (type === 'warning') typeClass = 'warning-line';

  line.className = `console-line ${typeClass}`;
  
  const timestamp = new Date().toLocaleTimeString();
  let logText = `[${timestamp}] ${msg}`;
  if (data) {
    logText += `\n${JSON.stringify(data, null, 2)}`;
  }

  line.textContent = logText;
  cdcConsoleFeed.appendChild(line);

  // Auto scroll to bottom
  cdcConsoleFeed.scrollTop = cdcConsoleFeed.scrollHeight;
}

// Health Metrics Poller
async function fetchMetrics() {
  try {
    const res = await fetch(`${API_BASE}/metrics`);
    if (!res.ok) return;

    const data = await res.json();

    // Update metrics widgets
    metricLag.textContent = `${parseFloat(data.oldestUnprocessedSeconds || 0).toFixed(2)}s`;
    metricClients.textContent = data.connectedClients;
    metricEps.textContent = `${parseFloat(data.eventsPerSecond || 0).toFixed(1)} eps`;

    // Circuit Breakers
    const dbState = data.circuitBreakers?.postgres || 'CLOSED';
    const cacheState = data.circuitBreakers?.redis || 'CLOSED';

    breakerDb.textContent = dbState;
    breakerDb.className = `status-badge-inline ${dbState.toLowerCase()}`;

    breakerCache.textContent = cacheState;
    breakerCache.className = `status-badge-inline ${cacheState.toLowerCase()}`;

  } catch (e) {
    // suppress errors to keep logs clean
  }
}

function startPoller() {
  fetchMetrics();
  metricsInterval = setInterval(fetchMetrics, 2000);
}

function stopPoller() {
  if (metricsInterval) {
    clearInterval(metricsInterval);
    metricsInterval = null;
  }
}

// Event Listeners Configuration
btnLoginAdmin.addEventListener('click', () => login('admin_user', 'admin'));
btnLoginCustomer.addEventListener('click', () => login('customer_a', 'customer'));

loginForm.addEventListener('submit', (e) => {
  e.preventDefault();
  login(usernameInput.value, roleSelect.value);
});

btnLogout.addEventListener('click', logout);

btnRefreshOrders.addEventListener('click', fetchOrdersSnapshot);

btnClearConsole.addEventListener('click', () => {
  cdcConsoleFeed.innerHTML = '';
});

// Network connection toggle handler
socketToggle.addEventListener('change', (e) => {
  if (e.target.checked) {
    connectSocket();
  } else {
    disconnectSocket();
  }
});

btnSyncReplay.addEventListener('click', () => {
  connectSocket();
});

orderForm.addEventListener('submit', (e) => {
  e.preventDefault();
  const customer = orderCustomerInput.value;
  const product = orderProductInput.value;
  const status = orderStatusSelect.value;
  createOrder(customer, product, status);
});

// Bootstrap
initSession();
