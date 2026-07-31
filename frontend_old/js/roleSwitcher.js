/**
 * roleSwitcher.js — FoodChain Dynamic Role System
 *
 * Responsibilities:
 *  · Maintain role configuration (colors, text, visibility rules)
 *  · Update CSS custom properties when role changes
 *  · Animate card content transitions (fade-out → update → fade-in)
 *  · Expose window.FoodChain.* API for other modules
 *
 * Dependencies: none (pure vanilla JS)
 */
(function () {
  'use strict';

  /* ════════════════════════════════════════════════════════════
     Role Configuration
  ════════════════════════════════════════════════════════════ */
  const ROLES = {

    consumer: {
      id:               'consumer',
      label:            'Consumer Login',
      subtitle:         'Scan and track food products',
      color:            '#22c55e',
      colorRgb:         '34, 197, 94',
      gradient:         'linear-gradient(135deg, #15803d 0%, #22c55e 55%, #10b981 100%)',
      iconClass:        'fa-regular fa-user',
      showCreateAcct:   false,
      showQR:           true,
    },

    admin: {
      id:               'admin',
      label:            'Administrator Login',
      subtitle:         'Manage the supply chain system',
      color:            '#3b82f6',
      colorRgb:         '59, 130, 246',
      gradient:         'linear-gradient(135deg, #1d4ed8 0%, #3b82f6 55%, #60a5fa 100%)',
      iconClass:        'fa-solid fa-shield-halved',
      showCreateAcct:   true,
      showQR:           false,
    },

    farmer: {
      id:               'farmer',
      label:            'Farmer Login',
      subtitle:         'Manage your farm & produce',
      color:            '#84cc16',
      colorRgb:         '132, 204, 22',
      gradient:         'linear-gradient(135deg, #4d7c0f 0%, #84cc16 55%, #a3e635 100%)',
      iconClass:        'fa-solid fa-seedling',
      showCreateAcct:   true,
      showQR:           false,
    },

    retailer: {
      id:               'retailer',
      label:            'Retailer Login',
      subtitle:         'Manage your store & inventory',
      color:            '#f97316',
      colorRgb:         '249, 115, 22',
      gradient:         'linear-gradient(135deg, #c2410c 0%, #f97316 55%, #fb923c 100%)',
      iconClass:        'fa-solid fa-store',
      showCreateAcct:   true,
      showQR:           false,
    },
  };

  /* ── State ─────────────────────────────────────────────────── */
  let current     = 'consumer';
  let transitioning = false;

  /* ── CSS Variable updater ───────────────────────────────────── */
  function applyCSSVars (cfg) {
    const root = document.documentElement;
    root.style.setProperty('--role-color',    cfg.color);
    root.style.setProperty('--role-rgb',      cfg.colorRgb);
    root.style.setProperty('--role-gradient', cfg.gradient);
  }

  /* ── DOM updaters ───────────────────────────────────────────── */
  function updateCardHeader (cfg) {
    // Title
    const title = document.getElementById('cardTitle');
    if (title) title.textContent = cfg.label;

    // Subtitle
    const sub = document.getElementById('cardSubtitle');
    if (sub) sub.textContent = cfg.subtitle;

    // Icon — bounce animation
    const icon = document.getElementById('cardIcon');
    if (icon) {
      icon.innerHTML = `<i class="${cfg.iconClass}" aria-hidden="true"></i>`;
      icon.classList.remove('fc-bounce');
      void icon.offsetWidth;          // force reflow
      icon.classList.add('fc-bounce');
    }
  }

  function updateVisibility (cfg) {
    // Create Account button
    const createBtn = document.getElementById('createAccountBtn');
    if (createBtn) createBtn.style.display = cfg.showCreateAcct ? 'flex' : 'none';

    // QR section
    const qrSection = document.getElementById('qrSection');
    if (qrSection) qrSection.style.display = cfg.showQR ? 'grid' : 'none';
  }

  function updateCardGlow (cfg) {
    const glow = document.querySelector('.card-glow');
    if (glow) glow.style.background = cfg.color;
  }

  function updateMouseGlow (cfg) {
    const mg = document.getElementById('mouse-glow');
    if (mg) {
      mg.style.background =
        `radial-gradient(circle, rgba(${cfg.colorRgb}, 0.05) 0%, transparent 68%)`;
    }
  }

  function updateDotAccents () {
    // The CSS variable change will cascade to .dot-pulse via --role-color
  }

  function updateRoleCards (newRole) {
    document.querySelectorAll('.role-card').forEach(card => {
      const isActive = card.dataset.role === newRole;
      card.classList.toggle('active', isActive);
      card.setAttribute('aria-selected', isActive ? 'true' : 'false');
    });
  }

  function updateHiddenInput (newRole) {
    const input = document.getElementById('selectedRole');
    if (input) input.value = newRole;
  }

  /* ── Transition ─────────────────────────────────────────────── */
  function switchRole (newRole) {
    if (!ROLES[newRole] || newRole === current || transitioning) return;
    transitioning = true;

    const card = document.getElementById('loginCard');

    // ── Phase 1: Exit animation ──
    card.classList.add('fc-exiting');

    setTimeout(() => {
      // ── Phase 2: Apply new role ──
      const cfg = ROLES[newRole];
      applyCSSVars(cfg);
      updateCardHeader(cfg);
      updateVisibility(cfg);
      updateCardGlow(cfg);
      updateMouseGlow(cfg);
      updateRoleCards(newRole);
      updateHiddenInput(newRole);
      current = newRole;

      // Clear error/success alerts on role switch
      const errorAlert   = document.getElementById('errorAlert');
      const successAlert = document.getElementById('successAlert');
      if (errorAlert)   errorAlert.style.display   = 'none';
      if (successAlert) successAlert.style.display = 'none';

      // ── Phase 3: Enter animation ──
      card.classList.remove('fc-exiting');
      card.classList.add('fc-entering');

      setTimeout(() => {
        card.classList.remove('fc-entering');
        transitioning = false;
      }, 350);

    }, 210);
  }

  /* ── Keyboard nav (arrow keys within role tab row) ──────────── */
  function setupArrowNav () {
    const cards = Array.from(document.querySelectorAll('.role-card'));
    cards.forEach((card, idx) => {
      card.addEventListener('keydown', (e) => {
        let next = idx;
        if (e.key === 'ArrowRight') next = (idx + 1) % cards.length;
        if (e.key === 'ArrowLeft')  next = (idx - 1 + cards.length) % cards.length;
        if (next !== idx) {
          e.preventDefault();
          cards[next].focus();
          switchRole(cards[next].dataset.role);
        }
      });
    });
  }

  /* ── Init ───────────────────────────────────────────────────── */
  function init () {
    // Click handlers
    document.querySelectorAll('.role-card').forEach(card => {
      card.addEventListener('click', () => {
        if (card.dataset.role) switchRole(card.dataset.role);
      });

      card.addEventListener('keydown', (e) => {
        if (e.key === 'Enter' || e.key === ' ') {
          e.preventDefault();
          if (card.dataset.role) switchRole(card.dataset.role);
        }
      });
    });

    setupArrowNav();

    // Apply initial role (consumer)
    const cfg = ROLES['consumer'];
    applyCSSVars(cfg);
    updateCardHeader(cfg);
    updateVisibility(cfg);
    updateCardGlow(cfg);
    updateMouseGlow(cfg);
    updateRoleCards('consumer');
    updateHiddenInput('consumer');
  }

  /* ── Public API ─────────────────────────────────────────────── */
  window.FoodChain           = window.FoodChain || {};
  window.FoodChain.ROLES     = ROLES;
  window.FoodChain.getRole   = () => current;
  window.FoodChain.setRole   = switchRole;

  /* ── Boot ───────────────────────────────────────────────────── */
  if (document.readyState === 'loading') {
    document.addEventListener('DOMContentLoaded', init);
  } else {
    init();
  }
})();
