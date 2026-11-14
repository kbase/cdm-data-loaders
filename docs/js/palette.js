(function () {
  var STORAGE_KEY = "__theme_colors";
  var FALLBACK_COLOR = "teal";

  var VALID_PRIMARY = new Set([
    "red",
    "pink",
    "purple",
    "deep-purple",
    "indigo",
    "blue",
    "light-blue",
    "cyan",
    "teal",
    "green",
    "light-green",
    "lime",
    "yellow",
    "amber",
    "orange",
    "deep-orange",
    "brown",
    "grey",
    "blue-grey"
  ]);

  var VALID_ACCENT = new Set([
    "red",
    "pink",
    "purple",
    "deep-purple",
    "indigo",
    "blue",
    "light-blue",
    "cyan",
    "teal",
    "green",
    "light-green",
    "lime",
    "yellow",
    "amber",
    "orange",
    "deep-orange"
  ]);

  function getStored(key) {
    if (typeof __md_get === "function") {
      try {
        return __md_get(key);
      } catch (_err) {
        return null;
      }
    }
    return null;
  }

  function setStored(key, value) {
    if (typeof __md_set === "function") {
      try {
        __md_set(key, value);
      } catch (_err) {
        // ignore storage quota or private-browsing errors
      }
    }
  }

  function resolveDefaults() {
    var body = document.body;
    if (!body) {
      return { primary: FALLBACK_COLOR, accent: FALLBACK_COLOR };
    }
    if (!body.dataset.cdmDefaultPrimary || !body.dataset.cdmDefaultAccent) {
      var firstOption = document.querySelector("[data-md-component='palette'] input.md-option");
      var defaultPrimary =
        (firstOption && firstOption.getAttribute("data-md-color-primary")) || FALLBACK_COLOR;
      var defaultAccent =
        (firstOption && firstOption.getAttribute("data-md-color-accent")) || FALLBACK_COLOR;
      body.dataset.cdmDefaultPrimary = VALID_PRIMARY.has(defaultPrimary)
        ? defaultPrimary
        : FALLBACK_COLOR;
      body.dataset.cdmDefaultAccent = VALID_ACCENT.has(defaultAccent)
        ? defaultAccent
        : FALLBACK_COLOR;
    }
    return {
      primary: body.dataset.cdmDefaultPrimary,
      accent: body.dataset.cdmDefaultAccent
    };
  }

  function currentColors() {
    var defaults = resolveDefaults();
    var saved = getStored(STORAGE_KEY);
    var primary =
      saved && typeof saved.primary === "string" && VALID_PRIMARY.has(saved.primary)
        ? saved.primary
        : defaults.primary;
    var accent =
      saved && typeof saved.accent === "string" && VALID_ACCENT.has(saved.accent)
        ? saved.accent
        : defaults.accent;
    return { primary: primary, accent: accent };
  }

  function syncPickerControls(primary, accent) {
    var primaryBadge = document.getElementById("cdm-active-primary-value");
    if (primaryBadge) {
      primaryBadge.textContent = primary;
    }
    var accentBadge = document.getElementById("cdm-active-accent-value");
    if (accentBadge) {
      accentBadge.textContent = accent;
    }
    var tomlCode = document.getElementById("cdm-palette-toml");
    if (tomlCode) {
      tomlCode.textContent =
        "[[project.theme.palette]]\n" +
        'primary = "' +
        primary +
        '"\n' +
        'accent = "' +
        accent +
        '"';
    }
    document.querySelectorAll("button[data-md-color-primary]").forEach(function (btn) {
      var isSelected = btn.getAttribute("data-md-color-primary") === primary;
      btn.setAttribute("aria-pressed", isSelected ? "true" : "false");
    });
    document.querySelectorAll("button[data-md-color-accent]").forEach(function (btn) {
      var isSelected = btn.getAttribute("data-md-color-accent") === accent;
      btn.setAttribute("aria-pressed", isSelected ? "true" : "false");
    });
  }

  function applyThemeColors(primary, accent, persist) {
    if (!document.body) {
      return;
    }
    var safePrimary = VALID_PRIMARY.has(primary) ? primary : resolveDefaults().primary;
    var safeAccent = VALID_ACCENT.has(accent) ? accent : resolveDefaults().accent;

    document.body.setAttribute("data-md-color-switching", "");
    document.body.setAttribute("data-md-color-primary", safePrimary);
    document.body.setAttribute("data-md-color-accent", safeAccent);

    document
      .querySelectorAll("[data-md-component='palette'] input.md-option")
      .forEach(function (input) {
        input.setAttribute("data-md-color-primary", safePrimary);
        input.setAttribute("data-md-color-accent", safeAccent);
      });

    var paletteState = getStored("__palette");
    if (paletteState && paletteState.color) {
      paletteState.color.primary = safePrimary;
      paletteState.color.accent = safeAccent;
      setStored("__palette", paletteState);
    }

    if (persist) {
      setStored(STORAGE_KEY, { primary: safePrimary, accent: safeAccent });
    }

    syncPickerControls(safePrimary, safeAccent);

    window.requestAnimationFrame(function () {
      if (document.body) {
        document.body.removeAttribute("data-md-color-switching");
      }
    });
  }

  function bindPaletteFormGuard() {
    var form = document.querySelector("[data-md-component='palette']");
    if (!form || form.dataset.cdmPaletteBound === "true") {
      return;
    }
    form.dataset.cdmPaletteBound = "true";
    form.addEventListener("change", function () {
      var active = currentColors();
      window.requestAnimationFrame(function () {
        applyThemeColors(active.primary, active.accent, false);
      });
    });
  }

  function bindPickerControls() {
    var root = document.querySelector("[data-cdm-color-picker]");
    if (!root || root.dataset.cdmBound === "true") {
      return;
    }
    root.dataset.cdmBound = "true";

    root.addEventListener("click", function (event) {
      var target = event.target instanceof Element ? event.target : null;
      if (!target) {
        return;
      }
      var primaryBtn = target.closest("button[data-md-color-primary]");
      if (primaryBtn) {
        var primaryVal = primaryBtn.getAttribute("data-md-color-primary") || "";
        if (VALID_PRIMARY.has(primaryVal)) {
          applyThemeColors(primaryVal, currentColors().accent, true);
        }
        return;
      }
      var accentBtn = target.closest("button[data-md-color-accent]");
      if (accentBtn) {
        var accentVal = accentBtn.getAttribute("data-md-color-accent") || "";
        if (VALID_ACCENT.has(accentVal)) {
          applyThemeColors(currentColors().primary, accentVal, true);
        }
        return;
      }
      var resetBtn = target.closest("#cdm-palette-reset");
      if (resetBtn) {
        var defaults = resolveDefaults();
        setStored(STORAGE_KEY, null);
        applyThemeColors(defaults.primary, defaults.accent, false);
      }
    });
  }

  function initPaletteCustomization() {
    resolveDefaults();
    var active = currentColors();
    applyThemeColors(active.primary, active.accent, false);
    bindPaletteFormGuard();
    bindPickerControls();
  }

  initPaletteCustomization();
  if (typeof document$ !== "undefined" && document$ && typeof document$.subscribe === "function") {
    document$.subscribe(initPaletteCustomization);
  }
})();
