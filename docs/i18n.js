/* ReDNS documentation — i18n engine + shared translations
 *
 * English text lives in each HTML page as the default content (and the
 * fallback for any missing key). Page-specific dictionaries are registered
 * via rednsI18nAdd() from per-section scripts (i18n/core.js,
 * i18n/matchers.js, i18n/executables.js) and are keyed by page id
 * (<body data-page="...">).
 *
 * Dictionary values may contain inline markup (<code>, <strong>, <a href=…>)
 * which is applied via innerHTML. Plain-text keys keep plain-text values.
 */
(function () {
  "use strict";

  /* ── Shared translations (present on every page) ─────────── */

  var SHARED = {
    "zh-CN": {
      "top.tagline": "DNS 转发器",

      "nav.home": "首页",
      "nav.quickstart": "快速开始",
      "nav.config": "配置",
      "nav.rules": "规则",
      "nav.plugins": "插件",
      "nav.upstreams": "上游",
      "nav.dashboard": "仪表盘",
      "nav.ops": "运维",
      "nav.faq": "FAQ",

      "crumb.home": "首页",
      "crumb.plugins": "插件",
      "crumb.matchers": "匹配器",
      "crumb.executables": "执行器",

      "common.yes": "是",
      "common.no": "否",
      "common.optional": "可选",
      "common.none": "无",

      "type.string": "字符串",
      "type.int": "整数",
      "type.float": "数字（浮点）",
      "type.bool": "布尔值",
      "type.list": "字符串列表",
      "type.maplist": "映射列表",
      "type.stringmap": "映射",
      "type.none": "无",
      "type.ms": "整数（毫秒）",
      "type.sec": "整数（秒）",
      "type.ip": "IP 地址",
      "type.cidr": "CIDR / IP 地址",
      "type.qname": "域名",
      "type.address": "地址（协议://主机:端口）",

      "tbl.h.param": "参数",
      "tbl.h.req": "必填",
      "tbl.h.type": "类型",
      "tbl.h.default": "默认值",
      "tbl.h.desc": "说明",
      "tbl.h.option": "选项",
      "tbl.h.example": "示例",

      "s.usage.t": "用法",
      "s.syntax.t": "语法",
      "s.params.t": "参数",
      "s.details.t": "行为细节",
      "s.examples.t": "示例",
      "s.related.t": "相关插件",

      "badge.matcher": "匹配器",
      "badge.executable": "执行器",
      "badge.recursive": "递归执行器",
      "badge.flow": "流程控制",
      "badge.server": "服务器插件",
      "badge.named": "具名插件",
      "badge.in-matches": "用于 matches",
      "badge.in-exec": "用于 exec",

      /* default-value phrases for the parameter tables */
      "dflt.info-stdout": "info / stdout",
      "dflt.empty-list": "空列表",
      "dflt.disabled": "缺省（禁用）",
      "dflt.asn-db": "无（自动下载默认数据库）",
      "dflt.auto-workers": "自动（2× CPU 核数，上限 32）",
      "dflt.sqlite-path": "<配置目录>/redns.db",
      "dflt.no-label": "缺省（无标签）",
      "dflt.resolve-host": "缺省（解析主机名）",
      "dflt.system-resolver": "缺省（系统解析器）",
      "dflt.pool-idle": "缺省（UDP 16 / 连接 4）",
      "dflt.memory-only": "缺省（仅内存）",
      "dflt.set-check": "任意值（仅检查变量已设置）",
      "dflt.required-except-zl": "无（除 zl 外必填）",
      "dflt.empty": "空",
      "dflt.no-effect": "自动（对 TCP 无效）",
      "dflt.n-a": "不适用",

      "footer.p": "ReDNS 文档 — English · 简体中文",
      "meta.description": "ReDNS 用户文档——一个用 Rust 编写的高性能 DNS 转发器。",
    },
  };

  var LANGS = ["en", "zh-CN"];
  var STORE_KEY = "redns-doc-lang";
  var THEME_KEY = "redns-doc-theme";
  var PAGES = {}; // lang -> { pageId -> dict }
  var booted = false;

  /* Original (English) content, snapshotted before any translation is applied.
   * Needed to switch BACK to English (or to restore missing keys): the English
   * text only exists in the HTML source, and innerHTML translation overwrites it. */
  var ORIGINALS = new Map();
  var ORIGINAL_ATTRS = new Map();
  var originalTitle = "";
  var originalMetaDesc = "";

  function currentLang() {
    try {
      var m = /[?&]lang=([^&]+)/.exec(location.search);
      if (m && LANGS.indexOf(m[1]) !== -1) return m[1];
    } catch (e) {}
    try {
      var saved = localStorage.getItem(STORE_KEY);
      if (saved && LANGS.indexOf(saved) !== -1) return saved;
    } catch (e) {}
    try {
      var nav = (navigator.language || "en").toLowerCase();
      if (nav.indexOf("zh") === 0) return "zh-CN";
    } catch (e) {}
    return "en";
  }

  function pageId() {
    var body = document.body;
    return body && body.getAttribute ? body.getAttribute("data-page") || "" : "";
  }

  /* Merged dictionary for the current page: shared + page-specific. */
  function currentDict(lang) {
    var d = {};
    if (SHARED[lang]) {
      for (var k in SHARED[lang]) d[k] = SHARED[lang][k];
    }
    var p = PAGES[lang] && PAGES[lang][pageId()];
    if (p) {
      for (var k2 in p) d[k2] = p[k2];
    }
    return d;
  }

  /* Snapshot the original (English) content before the first translation,
   * so switching back to English can restore it exactly. */
  function snapshotOriginals() {
    var nodes = document.querySelectorAll("[data-i18n]");
    for (var i = 0; i < nodes.length; i++) {
      ORIGINALS.set(nodes[i], nodes[i].innerHTML);
    }
    var attrs = document.querySelectorAll("[data-i18n-attr]");
    for (var j = 0; j < attrs.length; j++) {
      var spec = attrs[j].getAttribute("data-i18n-attr");
      var idx = spec.indexOf(":");
      var attr = spec.slice(0, idx);
      ORIGINAL_ATTRS.set(attrs[j], { attr: attr, value: attrs[j].getAttribute(attr) });
    }
    originalTitle = document.title;
    var m = document.querySelector('meta[name="description"]');
    if (m) originalMetaDesc = m.getAttribute("content");
  }

  function applyLang(lang) {
    var dict = currentDict(lang);
    var nodes = document.querySelectorAll("[data-i18n]");
    var attrs = document.querySelectorAll("[data-i18n-attr]");
    var i, el, key, spec, idx, attr;

    document.documentElement.lang = lang;

    for (i = 0; i < nodes.length; i++) {
      el = nodes[i];
      key = el.getAttribute("data-i18n");
      if (Object.prototype.hasOwnProperty.call(dict, key)) {
        el.innerHTML = dict[key];
      } else if (ORIGINALS.has(el)) {
        // Missing key (e.g. switching back to English): restore the original text.
        el.innerHTML = ORIGINALS.get(el);
      }
    }

    for (i = 0; i < attrs.length; i++) {
      el = attrs[i];
      spec = el.getAttribute("data-i18n-attr");
      idx = spec.indexOf(":");
      attr = spec.slice(0, idx);
      key = spec.slice(idx + 1);
      if (Object.prototype.hasOwnProperty.call(dict, key)) {
        el.setAttribute(attr, dict[key]);
      } else if (ORIGINAL_ATTRS.has(el)) {
        var orig = ORIGINAL_ATTRS.get(el);
        el.setAttribute(orig.attr, orig.value);
      }
    }

    if (dict["meta.title"]) {
      document.title = dict["meta.title"];
    } else if (originalTitle) {
      document.title = originalTitle;
    }
    if (dict["meta.description"]) {
      var m = document.querySelector('meta[name="description"]');
      if (m) m.setAttribute("content", dict["meta.description"]);
    } else if (originalMetaDesc) {
      var m2 = document.querySelector('meta[name="description"]');
      if (m2) m2.setAttribute("content", originalMetaDesc);
    }

    var btns = document.querySelectorAll("[data-lang]");
    for (i = 0; i < btns.length; i++) {
      btns[i].classList.toggle("active", btns[i].getAttribute("data-lang") === lang);
    }

    var pid = pageId();
    var navLinks = document.querySelectorAll("[data-nav]");
    var navActive = pid.indexOf("matcher-") === 0 || pid.indexOf("exec-") === 0 || pid === "plugin-sequence" ? "plugins" : pid;
    for (i = 0; i < navLinks.length; i++) {
      navLinks[i].classList.toggle("active", navLinks[i].getAttribute("data-nav") === navActive);
    }

    try {
      localStorage.setItem(STORE_KEY, lang);
    } catch (e) {}
  }

  /* Register per-page dictionaries: rednsI18nAdd({ "zh-CN": { pageId: {...} } }) */
  function rednsI18nAdd(dicts) {
    for (var lang in dicts) {
      if (!PAGES[lang]) PAGES[lang] = {};
      for (var page in dicts[lang]) {
        var cur = PAGES[lang][page] || {};
        var add = dicts[lang][page];
        for (var k in add) cur[k] = add[k];
        PAGES[lang][page] = cur;
      }
    }
    if (booted) applyLang(currentLang());
  }

  window.rednsI18nAdd = rednsI18nAdd;

  /* ── Theme ────────────────────────────────────────────────── */

  function applyTheme(theme) {
    document.documentElement.classList.toggle("dark", theme === "dark");
    document.documentElement.style.colorScheme = theme;
    try {
      localStorage.setItem(THEME_KEY, theme);
    } catch (e) {}
  }

  function detectTheme() {
    try {
      var saved = localStorage.getItem(THEME_KEY);
      if (saved === "dark" || saved === "light") return saved;
    } catch (e) {}
    return window.matchMedia && window.matchMedia("(prefers-color-scheme: dark)").matches
      ? "dark"
      : "light";
  }

  /* ── Sidebar scrollspy ────────────────────────────────────── */

  function initScrollspy() {
    var links = document.querySelectorAll('#toc a[href^="#"]');
    var sections = [];
    var i;
    for (i = 0; i < links.length; i++) {
      var id = links[i].getAttribute("href").slice(1);
      var sec = document.getElementById(id);
      if (sec) sections.push({ link: links[i], sec: sec });
    }
    if (!sections.length) return;

    function onScroll() {
      var pos = window.scrollY + 90;
      var current = sections[0];
      for (i = 0; i < sections.length; i++) {
        if (sections[i].sec.offsetTop <= pos) current = sections[i];
      }
      for (i = 0; i < sections.length; i++) {
        sections[i].link.classList.toggle("active", sections[i] === current);
      }
    }

    window.addEventListener("scroll", onScroll, { passive: true });
    onScroll();
  }

  /* ── Syntax tinting for code blocks ───────────────────────── */

  function highlightCode() {
    var pres = document.querySelectorAll("pre > code");
    var i, text, out;
    for (i = 0; i < pres.length; i++) {
      if (pres[i].getAttribute("data-highlighted")) continue;
      // Escape before re-injecting: the raw text may contain angle-bracket
      // placeholders (e.g. `[-c <config>]`) that would otherwise be parsed
      // as HTML tags when assigned back via innerHTML.
      text = pres[i].textContent
        .replace(/&/g, "&amp;")
        .replace(/</g, "&lt;")
        .replace(/>/g, "&gt;");
      out = text.replace(
        /("[^"\n]*")|(#[^\n]*)|(^|\s)([A-Za-z_][A-Za-z0-9_]*)(\s*:)|(\$[A-Za-z0-9_]+)|(\b\d+\b)/gm,
        function (m, str, comment, lead, key, colon, ref, num) {
          if (str) return '<span class="tok-str">' + str + "</span>";
          if (comment) return '<span class="tok-comment">' + comment + "</span>";
          if (key) {
            return lead + '<span class="tok-key">' + key + "</span>" + colon;
          }
          if (ref) return '<span class="tok-key">' + ref + "</span>";
          if (num) return '<span class="tok-punct">' + num + "</span>";
          return m;
        }
      );
      pres[i].innerHTML = out;
      pres[i].setAttribute("data-highlighted", "1");
    }
  }

  /* ── Copy buttons ─────────────────────────────────────────── */

  function initCopyButtons() {
    var pres = document.querySelectorAll("pre");
    var i;
    for (i = 0; i < pres.length; i++) {
      if (pres[i].querySelector(".copy-btn")) continue;
      var btn = document.createElement("button");
      btn.type = "button";
      btn.className = "copy-btn";
      btn.textContent = "\u2398";
      btn.title = "Copy";
      btn.addEventListener("click", function (b, pre) {
        return function () {
          var text = pre.querySelector("code").textContent;
          function done(ok) {
            b.textContent = ok ? "\u2713" : "\u2717";
            setTimeout(function () {
              b.textContent = "\u2398";
            }, 1200);
          }
          if (navigator.clipboard && navigator.clipboard.writeText) {
            navigator.clipboard.writeText(text).then(
              function () {
                done(true);
              },
              function () {
                done(false);
              }
            );
          } else {
            try {
              var ta = document.createElement("textarea");
              ta.value = text;
              document.body.appendChild(ta);
              ta.select();
              document.execCommand("copy");
              document.body.removeChild(ta);
              done(true);
            } catch (e) {
              done(false);
            }
          }
        };
      }(btn, pres[i]));
      pres[i].appendChild(btn);
    }
  }

  /* ── Boot ─────────────────────────────────────────────────── */

  function boot() {
    applyTheme(detectTheme());
    snapshotOriginals();
    applyLang(currentLang());
    highlightCode();
    initCopyButtons();
    initScrollspy();

    var i;
    var langBtns = document.querySelectorAll("[data-lang]");
    for (i = 0; i < langBtns.length; i++) {
      langBtns[i].addEventListener("click", function () {
        applyLang(this.getAttribute("data-lang"));
      });
    }

    var toggle = document.getElementById("theme-toggle");
    if (toggle) {
      toggle.addEventListener("click", function () {
        var dark = document.documentElement.classList.contains("dark");
        applyTheme(dark ? "light" : "dark");
      });
    }

    booted = true;
  }

  if (document.readyState === "loading") {
    document.addEventListener("DOMContentLoaded", boot);
  } else {
    boot();
  }
})();
