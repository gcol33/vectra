/* Shared canvas engine for the vectra vignette figures.
   Inlined by the `va-engine` chunk of every vignette that draws one. */
window.VA = (function () {

  var PALETTE = {
    dark: {
      bg: "#0f1320", grid: "#1b2237", off: "#1a2136",
      ink: "#dbe2f5", mut: "#8994b3",
      green: "#3ddc8a", red: "#ff5c7a", cyan: "#4cc9f0",
      amber: "#f0b429", purple: "#b388ff"
    },
    light: {
      bg: "#fbfcfe", grid: "#e6eaf2", off: "#eef1f7",
      ink: "#1b2230", mut: "#5c6577",
      green: "#0f8a55", red: "#c62348", cyan: "#0d6f96",
      amber: "#96650a", purple: "#6b46c1"
    }
  };

  var redraws = [];

  var VA = {
    ease: function (t) { return t < 0.5 ? 2 * t * t : 1 - Math.pow(-2 * t + 2, 2) / 2; },
    easeOut: function (t) { return 1 - Math.pow(1 - t, 3); },
    lerp: function (a, b, t) { return a + (b - a) * t; },
    clamp: function (v, a, b) { return Math.max(a, Math.min(b, v)); },

    /* Live palette. Mutated in place on a theme change, never replaced, so a
       draw body that captured `C = VA.C` keeps seeing current colours. */
    C: {},

    /* Palette colour at an alpha. */
    A: function (hex, a) {
      var r = parseInt(hex.slice(1, 3), 16),
          g = parseInt(hex.slice(3, 5), 16),
          b = parseInt(hex.slice(5, 7), 16);
      return "rgba(" + r + "," + g + "," + b + "," + a + ")";
    },

    F: function (sz, bold) { return (bold ? "bold " : "") + Math.round(sz * 1.25) + "px monospace"; },

    bg: function (c, w, h) {
      c.fillStyle = this.C.bg; c.fillRect(0, 0, w, h);
      c.strokeStyle = this.C.grid; c.lineWidth = 1;
      for (var gx = 26; gx < w; gx += 26) { c.beginPath(); c.moveTo(gx + 0.5, 0); c.lineTo(gx + 0.5, h); c.stroke(); }
      for (var gy = 26; gy < h; gy += 26) { c.beginPath(); c.moveTo(0, gy + 0.5); c.lineTo(w, gy + 0.5); c.stroke(); }
    },

    title: function (c, text) {
      c.fillStyle = this.C.ink; c.textAlign = "left"; c.font = this.F(12, true);
      c.fillText(text, 16, 26);
    },

    setup: function (id) {
      var cv = document.getElementById(id); if (!cv) return null;
      var ctx = cv.getContext("2d"), w = cv.width, h = cv.height, d = window.devicePixelRatio || 1;
      cv.width = w * d; cv.height = h * d; cv.style.width = w + "px"; cv.style.height = "auto";
      ctx.scale(d, d);
      return { ctx: ctx, w: w, h: h };
    },

    /* Draws the completed frame and rests there. The animation plays once when
       the figure scrolls into view, and again on click, then holds the
       completed frame -- so a reader who arrives at any moment sees the
       finished picture rather than a partial one. */
    run: function (drawAt, period, btnId, key, cvId) {
      var rest = period * 0.999;
      var frozen = (window.__FREEZE && (key in window.__FREEZE)) ? window.__FREEZE[key] : null;
      if (frozen != null) {
        var ft = ((frozen % period) + period) % period;
        redraws.push(function () { drawAt(ft); });
        drawAt(ft);
        return;
      }

      var raf = null, startedAt = 0;
      function frame(now) {
        var e = (now - startedAt) / 1000;
        if (e >= period) { raf = null; drawAt(rest); return; }
        drawAt(e);
        raf = requestAnimationFrame(frame);
      }
      function play() {
        if (raf != null) cancelAnimationFrame(raf);
        startedAt = performance.now();
        raf = requestAnimationFrame(frame);
      }

      redraws.push(function () { if (raf == null) drawAt(rest); });
      drawAt(rest);

      var cv = cvId ? document.getElementById(cvId) : null;
      if (cv) {
        cv.style.cursor = "pointer";
        cv.title = "click to replay";
        cv.addEventListener("click", play);
      }
      if (btnId) { var b = document.getElementById(btnId); if (b) b.addEventListener("click", play); }

      var still = window.matchMedia && window.matchMedia("(prefers-reduced-motion: reduce)").matches;
      if (!still && cv && window.IntersectionObserver) {
        var io = new IntersectionObserver(function (entries) {
          for (var i = 0; i < entries.length; i++) {
            if (entries[i].isIntersecting) { io.disconnect(); play(); return; }
          }
        }, { threshold: 0.25 });
        io.observe(cv);
      } else if (!still && !cv) {
        play();
      }
    }
  };

  /* pkgdown's light switch sets data-bs-theme. A standalone vignette has no
     switch, so fall back to the page's own background, then to the OS setting. */
  function readTheme() {
    var attr = document.documentElement.getAttribute("data-bs-theme");
    if (attr === "light" || attr === "dark") return attr;
    var el = document.body || document.documentElement;
    var m = /rgba?\((\d+),\s*(\d+),\s*(\d+)(?:,\s*([\d.]+))?\)/.exec(window.getComputedStyle(el).backgroundColor);
    if (m && !(m[4] !== undefined && parseFloat(m[4]) === 0)) {
      return (0.299 * +m[1] + 0.587 * +m[2] + 0.114 * +m[3]) / 255 < 0.5 ? "dark" : "light";
    }
    return (window.matchMedia && window.matchMedia("(prefers-color-scheme: dark)").matches) ? "dark" : "light";
  }

  function applyTheme() {
    var p = PALETTE[readTheme()];
    for (var k in p) VA.C[k] = p[k];
    for (var i = 0; i < redraws.length; i++) redraws[i]();
  }

  applyTheme();

  if (window.MutationObserver) {
    new MutationObserver(applyTheme).observe(document.documentElement,
      { attributes: true, attributeFilter: ["data-bs-theme"] });
  }
  if (window.matchMedia) {
    var mq = window.matchMedia("(prefers-color-scheme: dark)");
    if (mq.addEventListener) mq.addEventListener("change", applyTheme);
  }
  if (document.readyState === "loading") {
    document.addEventListener("DOMContentLoaded", applyTheme);
  }

  var style = document.createElement("style");
  style.textContent =
    "pre, pre.sourceCode, div.sourceCode{overflow-x:auto;max-width:100%}" +
    "canvas{display:block;margin:16px auto;max-width:100%;border-radius:8px;" +
    "border:1px solid rgba(125,138,170,0.28)}";
  document.head.appendChild(style);

  return VA;
})();
