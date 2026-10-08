/* ── Role-Based Client-Side Route Guard ── */

(function() {
  const currentRole = getRole();
  const token = getToken();
  
  // Find which page we are on
  const path = window.location.pathname;
  const page = path.substring(path.lastIndexOf('/') + 1) || 'home.html';
  
  // Skip auth checks on public pages
  const publicPages = ['login.html', 'home.html', 'track.html', 'qr.html'];
  if (publicPages.includes(page)) {
    return;
  }
  
  if (!token) {
    window.location.href = 'login.html';
    return;
  }
  
  // Admin can view every dashboard — no further checks needed
  if (currentRole === 'admin') {
    return;
  }

  // Everyone else is locked to exactly their own dashboard
  if (page === 'admin-dashboard.html') {
    redirectByRole();
  } else if (page === 'business-dashboard.html' && currentRole !== 'manager') {
    redirectByRole();
  } else if (page === 'producer-dashboard.html' && currentRole !== 'producer') {
    redirectByRole();
  } else if (page === 'distributor-dashboard.html' && currentRole !== 'distributor') {
    redirectByRole();
  }
})();
