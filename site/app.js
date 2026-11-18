// Small enhancements only: the page reads fine without this file.
(function () {
  "use strict";
  var root = document.documentElement;
  root.classList.add("js");

  var reduceMotion = window.matchMedia("(prefers-reduced-motion: reduce)").matches;
  var progress = document.querySelector(".progress-line");
  var heroProduct = document.querySelector(".hero-product");
  var story = document.querySelector(".product-story");
  var frame = document.querySelector(".product-frame");
  var steps = document.querySelectorAll(".product-steps span");

  // Scroll-driven bits: progress line, hero parallax, product-story angle.
  var ticking = false;
  function onScroll() {
    ticking = false;
    var y = window.scrollY;
    var max = document.documentElement.scrollHeight - window.innerHeight;
    if (progress) progress.style.transform = "scaleX(" + (max > 0 ? y / max : 0) + ")";

    if (heroProduct && !reduceMotion) {
      heroProduct.style.transform = "translateY(" + Math.min(y, 900) * -0.08 + "px)";
    }

    if (story && frame) {
      var rect = story.getBoundingClientRect();
      var travel = story.offsetHeight - window.innerHeight;
      var p = travel > 0 ? Math.min(Math.max(-rect.top / travel, 0), 0.999) : 0;
      var angle = String(Math.floor(p * 3));
      if (frame.getAttribute("data-angle") !== angle) {
        frame.setAttribute("data-angle", angle);
        steps.forEach(function (s, i) { s.classList.toggle("is-active", String(i) === angle); });
      }
    }
  }
  window.addEventListener("scroll", function () {
    if (!ticking) { ticking = true; window.requestAnimationFrame(onScroll); }
  }, { passive: true });
  window.addEventListener("resize", onScroll);
  onScroll();

  // Hero windows tilt a little toward the pointer (mouse and pen only).
  var hero = document.querySelector(".hero-scene");
  var stage = document.querySelector(".hero-stage");
  var front = document.querySelector(".win-front");
  if (hero && stage && !reduceMotion && window.matchMedia("(pointer: fine)").matches) {
    hero.addEventListener("pointermove", function (e) {
      var r = hero.getBoundingClientRect();
      var x = (e.clientX - r.left) / r.width - 0.5;
      var y = (e.clientY - r.top) / r.height - 0.5;
      stage.style.setProperty("--ry", (x * 12).toFixed(2) + "deg");
      stage.style.setProperty("--rx", (y * -8).toFixed(2) + "deg");
      if (front) {
        front.style.setProperty("--gx", (50 + x * 90).toFixed(1) + "%");
        front.style.setProperty("--gy", (30 + y * 90).toFixed(1) + "%");
      }
    });
    hero.addEventListener("pointerleave", function () {
      stage.style.setProperty("--ry", "0deg");
      stage.style.setProperty("--rx", "0deg");
    });
  }

  // Reveal on scroll, and count the metrics up once.
  function countUp(el) {
    var target = Number(el.getAttribute("data-count"));
    if (reduceMotion || !target) { el.textContent = String(target); return; }
    var start = null;
    function frameStep(t) {
      if (start === null) start = t;
      var k = Math.min((t - start) / 1150, 1);
      el.textContent = String(Math.round(target * (1 - Math.pow(1 - k, 3))));
      if (k < 1) window.requestAnimationFrame(frameStep);
    }
    el.textContent = "0";
    window.requestAnimationFrame(frameStep);
  }

  var revealables = document.querySelectorAll(".reveal, [data-count]");
  if (!("IntersectionObserver" in window)) {
    revealables.forEach(function (el) { el.classList.add("is-visible"); });
    return;
  }
  var observer = new IntersectionObserver(function (entries) {
    entries.forEach(function (entry) {
      if (!entry.isIntersecting) return;
      var el = entry.target;
      el.classList.add("is-visible");
      if (el.hasAttribute("data-count")) countUp(el);
      observer.unobserve(el);
    });
  }, { threshold: 0.18, rootMargin: "0px 0px -6% 0px" });
  revealables.forEach(function (el) { observer.observe(el); });
})();
