/* ── Authentication & Token Utilities for frontend_v2/ag ────────────────── */

const TOKEN_KEY = 'fc_token';
const ROLE_KEY  = 'fc_role';
const USER_KEY  = 'fc_username';

function getToken() {
  return localStorage.getItem(TOKEN_KEY) || sessionStorage.getItem(TOKEN_KEY);
}

function getRole() {
  return localStorage.getItem(ROLE_KEY) || sessionStorage.getItem(ROLE_KEY) || 'guest';
}

function getUsername() {
  return localStorage.getItem(USER_KEY) || sessionStorage.getItem(USER_KEY) || 'User';
}

function isLoggedIn() {
  return !!getToken();
}

function setSession(token, role, username, remember = true) {
  const store = remember ? localStorage : sessionStorage;
  store.setItem(TOKEN_KEY, token);
  store.setItem(ROLE_KEY, role);
  store.setItem(USER_KEY, username);
}

function logout() {
  localStorage.removeItem(TOKEN_KEY);
  localStorage.removeItem(ROLE_KEY);
  localStorage.removeItem(USER_KEY);
  sessionStorage.clear();
  window.location.href = 'login.html';
}

function checkAuth(allowGuest = false) {
  if (!isLoggedIn() && !allowGuest) {
    window.location.href = 'login.html';
    return false;
  }
  return true;
}

async function fetchWithAuth(url, options = {}) {
  const token = getToken();
  const headers = { ...options.headers };
  
  if (token) {
    headers['Authorization'] = `Bearer ${token}`;
  }
  
  try {
    const response = await fetch(url, { ...options, headers });
    if (response.status === 401) {
      logout();
      throw new Error('Session expired. Please log in again.');
    }
    return response;
  } catch (err) {
    console.warn('API Fetch error:', err);
    throw err;
  }
}
