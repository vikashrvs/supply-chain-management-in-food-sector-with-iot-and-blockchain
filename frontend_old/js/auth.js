const API_BASE = '';

function getToken() {
    return localStorage.getItem('auth_token');
}

function getRole() {
    return localStorage.getItem('role');
}

function isLoggedIn() {
    return !!getToken();
}

function logout() {
    localStorage.removeItem('auth_token');
    localStorage.removeItem('role');
    localStorage.removeItem('username');
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
    const headers = { ...options.headers, 'Content-Type': 'application/json' };
    if (token) {
        headers['Authorization'] = `Bearer ${token}`;
    }
    const response = await fetch(`${API_BASE}${url}`, { ...options, headers });
    if (response.status === 401) {
        logout();
        throw new Error('Session expired');
    }
    return response;
}
