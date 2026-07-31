/**
 * particles.js — FoodChain Login Background
 * Canvas-based animated particle system:
 *   · Starfield (glowing dots with pulse)
 *   · Blockchain network connection lines
 *   · Floating hexagon outlines
 *   · Mouse-tracking radial glow on canvas
 *
 * Self-contained IIFE — zero global leakage.
 */
(function () {
  'use strict';

  /* ── Configuration ────────────────────────────────────────── */
  const CFG = {
    particleCount: 85,
    connectDist:   140,     // px — max distance for line draw
    connectAlpha:  0.13,    // max opacity for connection lines
    hexCount:      7,
    hexRotSpeed:   0.0018,  // rad / frame
    bgColors:      ['#07111F', '#0D1B2A'],

    // Particle visual variety
    dotColors: [
      '#22c55e',  // green
      '#06b6d4',  // cyan
      '#3b82f6',  // blue
      '#60a5fa',  // light blue
      '#86efac',  // light green
      '#a5f3fc',  // light cyan
      '#ffffff',  // white
    ],
    lineColor: '#06b6d4',
  };

  /* ── State ─────────────────────────────────────────────────── */
  let canvas, ctx;
  let W = 0, H = 0;
  let particles  = [];
  let hexagons   = [];
  let mouse      = { x: -9999, y: -9999 };
  let rafId      = null;

  /* ════════════════════════════════════════════════════════════
     Particle
  ════════════════════════════════════════════════════════════ */
  class Particle {
    constructor (init = false) { this.spawn(init); }

    spawn (init = false) {
      this.x      = Math.random() * W;
      this.y      = init ? Math.random() * H : H + 10;
      this.vx     = (Math.random() - 0.5) * 0.28;
      this.vy     = -(0.08 + Math.random() * 0.22);
      this.r      = 0.6 + Math.random() * 1.8;
      this.glow   = 3  + Math.random() * 5;
      this.color  = CFG.dotColors[Math.floor(Math.random() * CFG.dotColors.length)];
      this.alpha  = 0.25 + Math.random() * 0.55;
      this.pSpeed = 0.012 + Math.random() * 0.022;
      this.pPhase = Math.random() * Math.PI * 2;
    }

    update () {
      this.x += this.vx;
      this.y += this.vy;
      this.pPhase += this.pSpeed;
      this.curAlpha = this.alpha * (0.65 + 0.35 * Math.sin(this.pPhase));

      // Wrap horizontal
      if (this.x < -6)  this.x = W + 6;
      if (this.x > W + 6) this.x = -6;
      // Recycle vertical
      if (this.y < -10) this.spawn();
    }

    draw () {
      // Soft glow halo
      const g = ctx.createRadialGradient(this.x, this.y, 0, this.x, this.y, this.glow * 3);
      g.addColorStop(0,   hexAlpha(this.color, this.curAlpha));
      g.addColorStop(0.5, hexAlpha(this.color, this.curAlpha * 0.35));
      g.addColorStop(1,   'transparent');

      ctx.beginPath();
      ctx.fillStyle = g;
      ctx.arc(this.x, this.y, this.glow * 3, 0, Math.PI * 2);
      ctx.fill();

      // Hard core
      ctx.beginPath();
      ctx.fillStyle = hexAlpha(this.color, this.curAlpha);
      ctx.arc(this.x, this.y, this.r, 0, Math.PI * 2);
      ctx.fill();
    }
  }

  /* ════════════════════════════════════════════════════════════
     Hexagon outline
  ════════════════════════════════════════════════════════════ */
  class Hexagon {
    constructor () {
      this.x     = Math.random() * W;
      this.y     = Math.random() * H;
      this.r     = 28 + Math.random() * 70;
      this.angle = Math.random() * Math.PI / 3;
      this.speed = (Math.random() - 0.5) * CFG.hexRotSpeed;
      this.alpha = 0.025 + Math.random() * 0.04;
      this.color = Math.random() > 0.5 ? '#22c55e' : '#06b6d4';
    }

    update () { this.angle += this.speed; }

    draw () {
      ctx.save();
      ctx.globalAlpha = this.alpha;
      ctx.strokeStyle = this.color;
      ctx.lineWidth   = 0.6;
      ctx.beginPath();
      for (let i = 0; i < 6; i++) {
        const a = this.angle + (i * Math.PI * 2 / 6);
        const x = this.x + this.r * Math.cos(a);
        const y = this.y + this.r * Math.sin(a);
        i === 0 ? ctx.moveTo(x, y) : ctx.lineTo(x, y);
      }
      ctx.closePath();
      ctx.stroke();
      ctx.restore();
    }
  }

  /* ── Helpers ────────────────────────────────────────────────── */
  function hexAlpha (hex, a) {
    // Convert #rrggbb to rgba(r,g,b,a)
    const r = parseInt(hex.slice(1, 3), 16);
    const g = parseInt(hex.slice(3, 5), 16);
    const b = parseInt(hex.slice(5, 7), 16);
    return `rgba(${r},${g},${b},${a.toFixed(3)})`;
  }

  /* ── Draw connections ───────────────────────────────────────── */
  function drawConnections () {
    const n = particles.length;
    for (let i = 0; i < n; i++) {
      for (let j = i + 1; j < n; j++) {
        const dx   = particles[i].x - particles[j].x;
        const dy   = particles[i].y - particles[j].y;
        const dist = Math.sqrt(dx * dx + dy * dy);

        if (dist < CFG.connectDist) {
          const alpha = (1 - dist / CFG.connectDist) * CFG.connectAlpha;
          ctx.save();
          ctx.globalAlpha = alpha;
          ctx.strokeStyle = CFG.lineColor;
          ctx.lineWidth   = 0.5;
          ctx.beginPath();
          ctx.moveTo(particles[i].x, particles[i].y);
          ctx.lineTo(particles[j].x, particles[j].y);
          ctx.stroke();
          ctx.restore();
        }
      }
    }
  }

  /* ── Draw background ────────────────────────────────────────── */
  function drawBackground () {
    const grad = ctx.createLinearGradient(0, 0, 0, H);
    grad.addColorStop(0, CFG.bgColors[0]);
    grad.addColorStop(1, CFG.bgColors[1]);
    ctx.fillStyle = grad;
    ctx.fillRect(0, 0, W, H);
  }

  /* ── Animate ────────────────────────────────────────────────── */
  function animate () {
    rafId = requestAnimationFrame(animate);

    drawBackground();

    // Hexagons
    hexagons.forEach(h => { h.update(); h.draw(); });

    // Network lines
    drawConnections();

    // Particles
    particles.forEach(p => { p.update(); p.draw(); });
  }

  /* ── Resize ─────────────────────────────────────────────────── */
  function resize () {
    if (!canvas) return;
    W = canvas.width  = window.innerWidth;
    H = canvas.height = window.innerHeight;
  }

  /* ── Mouse ──────────────────────────────────────────────────── */
  function onMouse (e) {
    mouse.x = e.clientX;
    mouse.y = e.clientY;

    // Move DOM glow overlay
    const glow = document.getElementById('mouse-glow');
    if (glow) {
      glow.style.left = mouse.x + 'px';
      glow.style.top  = mouse.y + 'px';
    }
  }

  /* ── Init ───────────────────────────────────────────────────── */
  function init () {
    canvas = document.getElementById('bg-canvas');
    if (!canvas) return;
    ctx = canvas.getContext('2d');

    resize();
    window.addEventListener('resize', resize, { passive: true });
    window.addEventListener('mousemove', onMouse, { passive: true });

    // Spawn entities
    for (let i = 0; i < CFG.particleCount; i++) particles.push(new Particle(true));
    for (let i = 0; i < CFG.hexCount;      i++) hexagons.push(new Hexagon());

    animate();
  }

  /* ── Boot ───────────────────────────────────────────────────── */
  if (document.readyState === 'loading') {
    document.addEventListener('DOMContentLoaded', init);
  } else {
    init();
  }
})();
