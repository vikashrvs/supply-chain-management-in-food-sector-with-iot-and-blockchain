/**
 * app.js — FoodChain Login Page: Main Application Logic
 *
 * Handles:
 *  · Form validation (real-time + on-submit)
 *  · Login API call (fetch, JSON)
 *  · Password visibility toggle with animation
 *  · Remember Me (localStorage persistence)
 *  · Button ripple effect
 *  · Loading / error / success states
 *  · Scan QR Code action
 *  · Track with Product ID action
 *  · Create Account routing
 *  · Keyboard shortcuts (Enter, Escape)
 *
 * Backend-ready: works with Node/Express, Django, Flask,
 * Spring Boot, Firebase — no frontend changes needed.
 * Just set AUTH_ENDPOINT and the server must respond with:
 * { access_token: "...", role: "...", username: "..." }
 *
 * IDs used by this module (referenced by backend integrators):
 *   #email  #password  #loginBtn  #roleSelector  #selectedRole
 *   #createAccountBtn  #scanQRBtn  #trackProductBtn
 */
(function () {
  'use strict';

  /* ── Config ─────────────────────────────────────────────────── */
  const AUTH_ENDPOINT    = '/api/auth/login';   // Change to your backend URL
  const DASHBOARD_URL    = 'dashboard.html';
  const REGISTER_URL     = 'register.html';
  const TRACK_URL        = 'track.html';
  const STORAGE_TOKEN    = 'fc_token';
  const STORAGE_USER     = 'fc_user';
  const STORAGE_ROLE     = 'fc_role';
  const STORAGE_REMEMBER = 'fc_remember_email';

  /* ── DOM Refs ────────────────────────────────────────────────── */
  const form            = document.getElementById('loginForm');
  const emailEl         = document.getElementById('email');
  const passwordEl      = document.getElementById('password');
  const eyeToggleEl     = document.getElementById('eyeToggle');
  const eyeIconEl       = document.getElementById('eyeIcon');
  const rememberMeEl    = document.getElementById('rememberMe');
  const loginBtnEl      = document.getElementById('loginBtn');
  const btnLabelEl      = document.getElementById('btnLabel');
  const btnSpinnerEl    = document.getElementById('btnSpinner');
  const btnArrowEl      = document.getElementById('btnArrow');
  const btnRippleEl     = document.querySelector('.btn-ripple');
  const errorAlertEl    = document.getElementById('errorAlert');
  const errorMsgEl      = document.getElementById('errorMsg');
  const successAlertEl  = document.getElementById('successAlert');
  const successMsgEl    = document.getElementById('successMsg');
  const selectedRoleEl  = document.getElementById('selectedRole');
  const scanQRBtnEl     = document.getElementById('scanQRBtn');
  const trackBtnEl      = document.getElementById('trackProductBtn');
  const createBtnEl     = document.getElementById('createAccountBtn');
  const forgotLinkEl    = document.getElementById('forgotLink');
  const emailErrorEl    = document.getElementById('emailError');
  const pwdErrorEl      = document.getElementById('passwordError');

  /* ── State ──────────────────────────────────────────────────── */
  let pwdVisible    = false;
  let isSubmitting  = false;

  /* ════════════════════════════════════════════════════════════
     Validation
  ════════════════════════════════════════════════════════════ */

  /** Accepts email OR 10-digit Indian mobile starting 6-9 */
  function isValidEmail (v) {
    return /^[^\s@]+@[^\s@]+\.[^\s@]+$/.test(v) || /^[6-9]\d{9}$/.test(v);
  }

  function showFieldErr (inputEl, errEl, msg) {
    if (inputEl) inputEl.classList.add('invalid');
    if (errEl)  { errEl.textContent = msg; errEl.classList.add('show'); }
  }

  function clearFieldErr (inputEl, errEl) {
    if (inputEl) inputEl.classList.remove('invalid');
    if (errEl)   errEl.classList.remove('show');
  }

  function validateForm () {
    let ok = true;
    const email = (emailEl?.value || '').trim();
    const pwd   = passwordEl?.value || '';

    if (!email) {
      showFieldErr(emailEl, emailErrorEl, 'Email or mobile number is required.');
      ok = false;
    } else if (!isValidEmail(email)) {
      showFieldErr(emailEl, emailErrorEl, 'Enter a valid email or 10-digit mobile number.');
      ok = false;
    } else {
      clearFieldErr(emailEl, emailErrorEl);
    }

    if (!pwd) {
      showFieldErr(passwordEl, pwdErrorEl, 'Password is required.');
      ok = false;
    } else if (pwd.length < 6) {
      showFieldErr(passwordEl, pwdErrorEl, 'Password must be at least 6 characters.');
      ok = false;
    } else {
      clearFieldErr(passwordEl, pwdErrorEl);
    }

    return ok;
  }

  /* ════════════════════════════════════════════════════════════
     Alert helpers
  ════════════════════════════════════════════════════════════ */

  function showError (msg) {
    hideSuccess();
    if (errorMsgEl) errorMsgEl.textContent = msg;
    if (errorAlertEl) {
      errorAlertEl.style.display = 'flex';
      errorAlertEl.scrollIntoView({ behavior: 'smooth', block: 'nearest' });
    }
  }

  function showSuccess (msg) {
    hideError();
    if (successMsgEl) successMsgEl.textContent = msg;
    if (successAlertEl) successAlertEl.style.display = 'flex';
  }

  function hideError   () { if (errorAlertEl)   errorAlertEl.style.display   = 'none'; }
  function hideSuccess () { if (successAlertEl) successAlertEl.style.display = 'none'; }
  function hideAlerts  () { hideError(); hideSuccess(); }

  /* ════════════════════════════════════════════════════════════
     Loading state
  ════════════════════════════════════════════════════════════ */

  function setLoading (loading) {
    isSubmitting = loading;

    if (!loginBtnEl) return;
    loginBtnEl.classList.toggle('is-loading', loading);
    loginBtnEl.disabled = loading;

    if (btnLabelEl)  btnLabelEl.textContent = loading ? 'Signing in...' : 'Login';
    if (btnArrowEl)  btnArrowEl.style.display  = loading ? 'none'  : 'flex';
    if (btnSpinnerEl) btnSpinnerEl.style.display = loading ? 'flex'  : 'none';
  }

  /* ════════════════════════════════════════════════════════════
     Password toggle
  ════════════════════════════════════════════════════════════ */

  function togglePwd () {
    pwdVisible = !pwdVisible;
    if (passwordEl) passwordEl.type = pwdVisible ? 'text' : 'password';

    if (eyeIconEl) {
      eyeIconEl.className = pwdVisible
        ? 'fa-regular fa-eye-slash'
        : 'fa-regular fa-eye';
    }

    // micro-bounce
    if (eyeToggleEl) {
      eyeToggleEl.style.transform = 'scale(0.78)';
      setTimeout(() => { if (eyeToggleEl) eyeToggleEl.style.transform = ''; }, 140);
    }
  }

  /* ════════════════════════════════════════════════════════════
     Ripple effect
  ════════════════════════════════════════════════════════════ */

  function triggerRipple (e) {
    if (!btnRippleEl || !loginBtnEl) return;
    const rect = loginBtnEl.getBoundingClientRect();
    const x = (e.clientX - rect.left);
    const y = (e.clientY - rect.top);

    btnRippleEl.style.left = x + 'px';
    btnRippleEl.style.top  = y + 'px';
    btnRippleEl.classList.remove('is-rippling');
    void btnRippleEl.offsetWidth;
    btnRippleEl.classList.add('is-rippling');
  }

  /* ════════════════════════════════════════════════════════════
     Remember Me
  ════════════════════════════════════════════════════════════ */

  function loadRemembered () {
    const saved = localStorage.getItem(STORAGE_REMEMBER);
    if (saved && emailEl) {
      emailEl.value = saved;
      if (rememberMeEl) rememberMeEl.checked = true;
    }
  }

  function persistRemember (email) {
    if (rememberMeEl?.checked) {
      localStorage.setItem(STORAGE_REMEMBER, email);
    } else {
      localStorage.removeItem(STORAGE_REMEMBER);
    }
  }

  /* ════════════════════════════════════════════════════════════
     Login submission
  ════════════════════════════════════════════════════════════ */

  async function handleLogin (e) {
    e.preventDefault();
    if (isSubmitting) return;

    hideAlerts();
    clearFieldErr(emailEl,    emailErrorEl);
    clearFieldErr(passwordEl, pwdErrorEl);

    if (!validateForm()) return;

    const email    = emailEl.value.trim();
    const password = passwordEl.value;
    const role     = selectedRoleEl?.value || 'consumer';

    setLoading(true);

    try {
      const res = await fetch(AUTH_ENDPOINT, {
        method:      'POST',
        credentials: 'same-origin',
        headers: {
          'Content-Type': 'application/json',
          'Accept':        'application/json',
        },
        body: JSON.stringify({ email, password, role }),
      });

      if (!res.ok) {
        // Try to parse server error message
        const payload = await res.json().catch(() => ({}));
        const msg = payload.detail || payload.message || payload.error
          || `Authentication failed (HTTP ${res.status}).`;
        throw new Error(msg);
      }

      const data = await res.json();

      // Persist session
      const store = rememberMeEl?.checked ? localStorage : sessionStorage;
      store.setItem(STORAGE_TOKEN, data.access_token || data.token || '');
      store.setItem(STORAGE_ROLE,  data.role || role);
      store.setItem(STORAGE_USER,  JSON.stringify({
        username: data.username || data.name || email,
        email:    data.email    || email,
        role:     data.role     || role,
      }));

      persistRemember(email);

      showSuccess('Login successful! Redirecting to dashboard…');

      setTimeout(() => {
        window.location.href = DASHBOARD_URL;
      }, 1100);

    } catch (err) {
      // Network error vs. server error
      const msg = (err.name === 'TypeError' || err.message.toLowerCase().includes('fetch'))
        ? 'Cannot connect to server. Please check your connection or try again.'
        : err.message;

      showError(msg);
      setLoading(false);

      // Shake the login card briefly
      const card = document.getElementById('loginCard');
      if (card) {
        card.style.transform = 'translateX(-4px)';
        setTimeout(() => { card.style.transform = 'translateX(4px)'; }, 80);
        setTimeout(() => { card.style.transform = ''; }, 160);
        card.style.transition = 'transform 0.08s ease';
        setTimeout(() => { card.style.transition = ''; }, 400);
      }
    }
  }

  /* ════════════════════════════════════════════════════════════
     QR / Track / Create Account
  ════════════════════════════════════════════════════════════ */

  function handleScanQR () {
    // Production: integrate with html5-qrcode or device camera API
    // For now, redirect to the dedicated QR scanning page
    window.location.href = 'qr.html';
  }

  function handleTrackProduct () {
    const uid = window.prompt('Enter Product ID to track:');
    if (uid && uid.trim()) {
      window.location.href = `${TRACK_URL}?uid=${encodeURIComponent(uid.trim())}`;
    }
  }

  function handleCreateAccount () {
    const role = selectedRoleEl?.value || 'admin';
    window.location.href = `${REGISTER_URL}?role=${encodeURIComponent(role)}`;
  }

  function handleForgotPassword () {
    // Production: implement forgot-password flow
    window.location.href = `forgot-password.html`;
  }

  /* ════════════════════════════════════════════════════════════
     Real-time input listeners
  ════════════════════════════════════════════════════════════ */

  function setupInputListeners () {
    if (emailEl) {
      emailEl.addEventListener('input', () => clearFieldErr(emailEl, emailErrorEl));
      emailEl.addEventListener('blur', () => {
        const v = emailEl.value.trim();
        if (v && !isValidEmail(v)) {
          showFieldErr(emailEl, emailErrorEl, 'Enter a valid email or 10-digit mobile number.');
        }
      });
    }

    if (passwordEl) {
      passwordEl.addEventListener('input', () => clearFieldErr(passwordEl, pwdErrorEl));
    }
  }

  /* ════════════════════════════════════════════════════════════
     Global keyboard shortcuts
  ════════════════════════════════════════════════════════════ */

  function setupKeyboard () {
    document.addEventListener('keydown', (e) => {
      // Escape — clear alerts
      if (e.key === 'Escape') hideAlerts();
    });
  }

  /* ════════════════════════════════════════════════════════════
     Init
  ════════════════════════════════════════════════════════════ */

  function init () {
    if (form)         form.addEventListener('submit', handleLogin);
    if (eyeToggleEl)  eyeToggleEl.addEventListener('click', togglePwd);
    if (loginBtnEl)   loginBtnEl.addEventListener('click', triggerRipple);
    if (scanQRBtnEl)  scanQRBtnEl.addEventListener('click', handleScanQR);
    if (trackBtnEl)   trackBtnEl.addEventListener('click', handleTrackProduct);
    if (createBtnEl)  createBtnEl.addEventListener('click', handleCreateAccount);
    if (forgotLinkEl) forgotLinkEl.addEventListener('click', (e) => {
      e.preventDefault();
      handleForgotPassword();
    });

    setupInputListeners();
    setupKeyboard();
    loadRemembered();

    /* Expose minimal API */
    window.FoodChain           = window.FoodChain || {};
    window.FoodChain.login     = handleLogin;
    window.FoodChain.scanQR    = handleScanQR;
    window.FoodChain.trackProd = handleTrackProduct;

    console.info('[FoodChain] Login page ready.');
  }

  if (document.readyState === 'loading') {
    document.addEventListener('DOMContentLoaded', init);
  } else {
    init();
  }
})();
