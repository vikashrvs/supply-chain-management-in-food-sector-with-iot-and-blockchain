(function () {
  const TOKEN_KEY = 'fc_token';
  const ROLE_KEY = 'fc_role';
  const USER_KEY = 'fc_username';

  function getApiBaseUrl() {
    const origin = window.location.origin;
    if (origin && origin !== 'null' && !origin.startsWith('file:')) {
      return origin;
    }
    // Default local backend port (matching FastAPI running port 8001)
    return 'http://127.0.0.1:8001';
  }

  function resolveApiUrl(path) {
    if (/^https?:\/\//i.test(path)) return path;
    const normalized = path && path.startsWith('/') ? path : '/' + (path || '');
    return getApiBaseUrl() + normalized;
  }

  function getToken() {
    return localStorage.getItem(TOKEN_KEY) || sessionStorage.getItem(TOKEN_KEY);
  }

  function getRole() {
    return localStorage.getItem(ROLE_KEY) || sessionStorage.getItem(ROLE_KEY) || 'guest';
  }

  function getUsername() {
    return localStorage.getItem(USER_KEY) || sessionStorage.getItem(USER_KEY) || 'User';
  }

  function setSession(token, role, username, remember = true) {
    const store = remember ? localStorage : sessionStorage;
    store.setItem(TOKEN_KEY, token);
    store.setItem(ROLE_KEY, (role || 'guest').toLowerCase());
    store.setItem(USER_KEY, username || 'User');
  }

  function logout() {
    localStorage.removeItem(TOKEN_KEY);
    localStorage.removeItem(ROLE_KEY);
    localStorage.removeItem(USER_KEY);
    sessionStorage.clear();
    window.location.href = 'login.html';
  }

  function redirectByRole() {
    const role = (getRole() || '').toLowerCase();
    if (role === 'admin') {
      window.location.href = 'admin-dashboard.html';
    } else if (role === 'producer') {
      window.location.href = 'producer-dashboard.html';
    } else if (role === 'distributor') {
      window.location.href = 'distributor-dashboard.html';
    } else if (role === 'business' || role === 'manager') {
      window.location.href = 'business-dashboard.html';
    } else {
      window.location.href = 'login.html';
    }
  }

  function initRoleGuard(expectedRole) {
    const token = getToken();
    const role = (getRole() || '').toLowerCase();

    // If no token, allow bypass only if opened as pure local file without backend, or redirect
    if (!token && window.location.protocol !== 'file:') {
      console.warn('No active session token found. Redirecting to login.html');
      window.location.href = 'login.html';
      return false;
    }

    const businessViewer = expectedRole === 'business' && (role === 'business' || role === 'manager' || role === 'admin');
    const adminViewer = role === 'admin';
    if (expectedRole && role !== 'guest' && role !== expectedRole.toLowerCase() && !businessViewer && !adminViewer) {
      console.warn(`Role mismatch: Expected ${expectedRole}, current is ${role}. Redirecting.`);
      redirectByRole();
      return false;
    }

    // Auto-populate user display in DOM if elements exist
    document.addEventListener('DOMContentLoaded', () => {
      applyUserInfoToDOM();
      wireLogoutButtons();
    });

    if (document.readyState !== 'loading') {
      applyUserInfoToDOM();
      wireLogoutButtons();
    }

    return true;
  }

  function applyUserInfoToDOM() {
    const uname = getUsername();
    const initial = (uname.charAt(0) || 'U').toUpperCase();

    // Look for common user name/avatar elements
    const nameEls = document.querySelectorAll('.user-name, #user-name, [data-user-name]');
    nameEls.forEach(el => { el.textContent = uname; });

    const avatarEls = document.querySelectorAll('.user-avatar, #user-avatar, .user-avatar-initial');
    avatarEls.forEach(el => {
      if (el.tagName === 'SPAN' || el.tagName === 'DIV') {
        el.textContent = initial;
      }
    });
  }

  function wireLogoutButtons() {
    const logoutBtns = document.querySelectorAll('#logout-btn, .logout-btn, [data-action="logout"]');
    logoutBtns.forEach(btn => {
      btn.onclick = (e) => {
        e.preventDefault();
        logout();
      };
    });
  }

  function ensureStatusBanner() {
    const existing = document.getElementById('api-status-banner');
    if (existing) existing.remove();
    return null;
  }

  function setApiStatus(message, kind) {
    const sidebarStatus = document.getElementById('sidebar-system-status');
    if (sidebarStatus) {
      sidebarStatus.textContent = message;
      sidebarStatus.dataset.status = kind || 'info';
      return;
    }
    let banner = document.getElementById('api-status-banner');
    if (!banner) {
      banner = document.createElement('div');
      banner.id = 'api-status-banner';
      banner.setAttribute('role', 'status');
      banner.setAttribute('aria-live', 'polite');
      // Inline styles so the banner floats above the page and never becomes a
      // grid/flex item of <body> (that was breaking the admin dashboard layout).
      banner.style.cssText = 'position:fixed;bottom:16px;right:16px;z-index:2000;'
        + 'max-width:min(420px,calc(100vw - 32px));padding:9px 13px;'
        + 'border:1px solid rgba(148,163,184,.35);border-radius:8px;'
        + 'background:rgba(15,23,42,.94);color:#dbeafe;font:600 12px/1.35 sans-serif;'
        + 'box-shadow:0 8px 24px rgba(0,0,0,.25);pointer-events:none;';
      document.body.appendChild(banner);
    }
    banner.textContent = message;
    banner.dataset.status = kind || 'info';
    if (kind === 'success') {
      banner.style.borderColor = 'rgba(34,197,94,.65)';
      banner.style.color = '#bbf7d0';
    } else if (kind === 'warning') {
      banner.style.borderColor = 'rgba(245,158,11,.7)';
      banner.style.color = '#fde68a';
    }
  }

  function formatHash(value, prefixLength = 8, suffixLength = 8) {
    const raw = String(value || '');
    if (!raw) return 'Not recorded';
    if (raw.length <= prefixLength + suffixLength + 3) return raw;
    return `${raw.slice(0, prefixLength)}…${raw.slice(-suffixLength)}`;
  }

  async function copyValue(value) {
    const raw = String(value || '');
    if (!raw) return false;
    await navigator.clipboard.writeText(raw);
    return true;
  }

  async function fetchWithAuth(url, options) {
    const token = getToken();
    const headers = { ...((options && options.headers) || {}) };
    if (token) headers.Authorization = 'Bearer ' + token;
    const response = await fetch(url, { ...(options || {}), headers });
    if (response.status === 401) {
      if (token) logout();
      throw new Error('Unauthorized. Please sign in again.');
    }
    return response;
  }

  async function fetchJson(path, options) {
    const response = await fetchWithAuth(resolveApiUrl(path), options);
    if (response.status === 204) return null;
    const contentType = response.headers.get('content-type') || '';
    const payload = contentType.includes('application/json') ? await response.json() : await response.text();
    if (!response.ok) {
      const detail = payload && typeof payload === 'object' ? (payload.detail || payload.message || payload.error) : payload;
      throw new Error(detail || 'Request failed');
    }
    return payload;
  }

  async function postJson(path, payload, options) {
    return fetchJson(path, {
      ...(options || {}),
      method: 'POST',
      headers: { 'Content-Type': 'application/json', ...((options && options.headers) || {}) },
      body: JSON.stringify(payload)
    });
  }

  window.FoodChainAPI = {
    getApiBaseUrl,
    resolveApiUrl,
    getToken,
    getRole,
    getUsername,
    setSession,
    logout,
    redirectByRole,
    initRoleGuard,
    setApiStatus,
    formatHash,
    copyValue,
    fetchWithAuth,
    fetchJson,
    postJson
  };
  window.fetchJson = fetchJson;
  window.postJson = postJson;
  window.setApiStatus = setApiStatus;
  window.logout = logout;
})();
