# About These Docs

This documentation site is built with [Zensical](https://zensical.org/) and kept synchronized with the codebase through dynamic Jinja2 macros and automated verification tests:

- **Site and navigation configuration**: [`zensical.toml`](https://github.com/kbase/cdm-data-loaders/blob/develop/zensical.toml) defines the top-level navigation tabs, light and dark color schemes, Python API reference generation (`mkdocstrings` and `griffe-pydantic`), and strict link and anchor validation.
- **Dynamic macro rendering**: [`docs_macros`](https://github.com/kbase/cdm-data-loaders/blob/develop/docs_macros) and Jinja2 templates in [`docs/_templates`](https://github.com/kbase/cdm-data-loaders/blob/develop/docs/_templates) render the [CLI Reference](cli_reference.md), [Bundled Schemas and XSD Assets](bundled_schemas.md), and all converter and pipeline specification pages directly from Pydantic settings classes, bundled schema files, and YAML manifests under [`specs`](https://github.com/kbase/cdm-data-loaders/blob/develop/specs).
- **Automated drift verification**: [`tests/test_docs_templates.py`](https://github.com/kbase/cdm-data-loaders/blob/develop/tests/test_docs_templates.py) validates every CLI flag, `CDL_*` environment variable, Python import, `blob/develop` repository link, and navigation entry on every pull request. See the [Development and Testing Guide](development_and_testing.md#building-and-verifying-documentation) for commands to preview and build the site locally.

---

## Theme Color Customization

Select a **primary color** (applied to inline links, active table-of-contents items, active navigation tab underlines, and primary buttons) and an **accent color** (applied to active sidebar highlights, link and button hover states, tabbed content indicators, and diagram accents). Your choices apply immediately and persist in browser local storage across pages and light or dark mode switches.

<div class="cdm-palette-picker" data-cdm-color-picker>
  <div class="cdm-palette-toolbar">
    <div class="cdm-palette-status">
      <span><strong>Primary color:</strong> <code id="cdm-active-primary-value">teal</code></span>
      <span><strong>Accent color:</strong> <code id="cdm-active-accent-value">teal</code></span>
    </div>
    <button type="button" class="cdm-palette-reset" id="cdm-palette-reset">
      Reset to default (teal)
    </button>
  </div>

  <h3>Primary Color</h3>
  <div class="cdm-swatch-grid" role="group" aria-label="Primary color">
    <button type="button" class="cdm-color-swatch" data-md-color-primary="red" aria-pressed="false"><span class="cdm-color-swatch__dot"></span><span class="cdm-color-swatch__label">Red</span><span class="cdm-color-swatch__check" aria-hidden="true">&#10003;</span></button>
    <button type="button" class="cdm-color-swatch" data-md-color-primary="pink" aria-pressed="false"><span class="cdm-color-swatch__dot"></span><span class="cdm-color-swatch__label">Pink</span><span class="cdm-color-swatch__check" aria-hidden="true">&#10003;</span></button>
    <button type="button" class="cdm-color-swatch" data-md-color-primary="purple" aria-pressed="false"><span class="cdm-color-swatch__dot"></span><span class="cdm-color-swatch__label">Purple</span><span class="cdm-color-swatch__check" aria-hidden="true">&#10003;</span></button>
    <button type="button" class="cdm-color-swatch" data-md-color-primary="deep-purple" aria-pressed="false"><span class="cdm-color-swatch__dot"></span><span class="cdm-color-swatch__label">Deep Purple</span><span class="cdm-color-swatch__check" aria-hidden="true">&#10003;</span></button>
    <button type="button" class="cdm-color-swatch" data-md-color-primary="indigo" aria-pressed="false"><span class="cdm-color-swatch__dot"></span><span class="cdm-color-swatch__label">Indigo</span><span class="cdm-color-swatch__check" aria-hidden="true">&#10003;</span></button>
    <button type="button" class="cdm-color-swatch" data-md-color-primary="blue" aria-pressed="false"><span class="cdm-color-swatch__dot"></span><span class="cdm-color-swatch__label">Blue</span><span class="cdm-color-swatch__check" aria-hidden="true">&#10003;</span></button>
    <button type="button" class="cdm-color-swatch" data-md-color-primary="light-blue" aria-pressed="false"><span class="cdm-color-swatch__dot"></span><span class="cdm-color-swatch__label">Light Blue</span><span class="cdm-color-swatch__check" aria-hidden="true">&#10003;</span></button>
    <button type="button" class="cdm-color-swatch" data-md-color-primary="cyan" aria-pressed="false"><span class="cdm-color-swatch__dot"></span><span class="cdm-color-swatch__label">Cyan</span><span class="cdm-color-swatch__check" aria-hidden="true">&#10003;</span></button>
    <button type="button" class="cdm-color-swatch" data-md-color-primary="teal" aria-pressed="true"><span class="cdm-color-swatch__dot"></span><span class="cdm-color-swatch__label">Teal</span><span class="cdm-color-swatch__check" aria-hidden="true">&#10003;</span></button>
    <button type="button" class="cdm-color-swatch" data-md-color-primary="green" aria-pressed="false"><span class="cdm-color-swatch__dot"></span><span class="cdm-color-swatch__label">Green</span><span class="cdm-color-swatch__check" aria-hidden="true">&#10003;</span></button>
    <button type="button" class="cdm-color-swatch" data-md-color-primary="light-green" aria-pressed="false"><span class="cdm-color-swatch__dot"></span><span class="cdm-color-swatch__label">Light Green</span><span class="cdm-color-swatch__check" aria-hidden="true">&#10003;</span></button>
    <button type="button" class="cdm-color-swatch" data-md-color-primary="lime" aria-pressed="false"><span class="cdm-color-swatch__dot"></span><span class="cdm-color-swatch__label">Lime</span><span class="cdm-color-swatch__check" aria-hidden="true">&#10003;</span></button>
    <button type="button" class="cdm-color-swatch" data-md-color-primary="yellow" aria-pressed="false"><span class="cdm-color-swatch__dot"></span><span class="cdm-color-swatch__label">Yellow</span><span class="cdm-color-swatch__check" aria-hidden="true">&#10003;</span></button>
    <button type="button" class="cdm-color-swatch" data-md-color-primary="amber" aria-pressed="false"><span class="cdm-color-swatch__dot"></span><span class="cdm-color-swatch__label">Amber</span><span class="cdm-color-swatch__check" aria-hidden="true">&#10003;</span></button>
    <button type="button" class="cdm-color-swatch" data-md-color-primary="orange" aria-pressed="false"><span class="cdm-color-swatch__dot"></span><span class="cdm-color-swatch__label">Orange</span><span class="cdm-color-swatch__check" aria-hidden="true">&#10003;</span></button>
    <button type="button" class="cdm-color-swatch" data-md-color-primary="deep-orange" aria-pressed="false"><span class="cdm-color-swatch__dot"></span><span class="cdm-color-swatch__label">Deep Orange</span><span class="cdm-color-swatch__check" aria-hidden="true">&#10003;</span></button>
    <button type="button" class="cdm-color-swatch" data-md-color-primary="brown" aria-pressed="false"><span class="cdm-color-swatch__dot"></span><span class="cdm-color-swatch__label">Brown</span><span class="cdm-color-swatch__check" aria-hidden="true">&#10003;</span></button>
    <button type="button" class="cdm-color-swatch" data-md-color-primary="grey" aria-pressed="false"><span class="cdm-color-swatch__dot"></span><span class="cdm-color-swatch__label">Grey</span><span class="cdm-color-swatch__check" aria-hidden="true">&#10003;</span></button>
    <button type="button" class="cdm-color-swatch" data-md-color-primary="blue-grey" aria-pressed="false"><span class="cdm-color-swatch__dot"></span><span class="cdm-color-swatch__label">Blue Grey</span><span class="cdm-color-swatch__check" aria-hidden="true">&#10003;</span></button>
  </div>

  <h3>Accent Color</h3>
  <div class="cdm-swatch-grid" role="group" aria-label="Accent color">
    <button type="button" class="cdm-color-swatch" data-md-color-accent="red" aria-pressed="false"><span class="cdm-color-swatch__dot"></span><span class="cdm-color-swatch__label">Red</span><span class="cdm-color-swatch__check" aria-hidden="true">&#10003;</span></button>
    <button type="button" class="cdm-color-swatch" data-md-color-accent="pink" aria-pressed="false"><span class="cdm-color-swatch__dot"></span><span class="cdm-color-swatch__label">Pink</span><span class="cdm-color-swatch__check" aria-hidden="true">&#10003;</span></button>
    <button type="button" class="cdm-color-swatch" data-md-color-accent="purple" aria-pressed="false"><span class="cdm-color-swatch__dot"></span><span class="cdm-color-swatch__label">Purple</span><span class="cdm-color-swatch__check" aria-hidden="true">&#10003;</span></button>
    <button type="button" class="cdm-color-swatch" data-md-color-accent="deep-purple" aria-pressed="false"><span class="cdm-color-swatch__dot"></span><span class="cdm-color-swatch__label">Deep Purple</span><span class="cdm-color-swatch__check" aria-hidden="true">&#10003;</span></button>
    <button type="button" class="cdm-color-swatch" data-md-color-accent="indigo" aria-pressed="false"><span class="cdm-color-swatch__dot"></span><span class="cdm-color-swatch__label">Indigo</span><span class="cdm-color-swatch__check" aria-hidden="true">&#10003;</span></button>
    <button type="button" class="cdm-color-swatch" data-md-color-accent="blue" aria-pressed="false"><span class="cdm-color-swatch__dot"></span><span class="cdm-color-swatch__label">Blue</span><span class="cdm-color-swatch__check" aria-hidden="true">&#10003;</span></button>
    <button type="button" class="cdm-color-swatch" data-md-color-accent="light-blue" aria-pressed="false"><span class="cdm-color-swatch__dot"></span><span class="cdm-color-swatch__label">Light Blue</span><span class="cdm-color-swatch__check" aria-hidden="true">&#10003;</span></button>
    <button type="button" class="cdm-color-swatch" data-md-color-accent="cyan" aria-pressed="false"><span class="cdm-color-swatch__dot"></span><span class="cdm-color-swatch__label">Cyan</span><span class="cdm-color-swatch__check" aria-hidden="true">&#10003;</span></button>
    <button type="button" class="cdm-color-swatch" data-md-color-accent="teal" aria-pressed="true"><span class="cdm-color-swatch__dot"></span><span class="cdm-color-swatch__label">Teal</span><span class="cdm-color-swatch__check" aria-hidden="true">&#10003;</span></button>
    <button type="button" class="cdm-color-swatch" data-md-color-accent="green" aria-pressed="false"><span class="cdm-color-swatch__dot"></span><span class="cdm-color-swatch__label">Green</span><span class="cdm-color-swatch__check" aria-hidden="true">&#10003;</span></button>
    <button type="button" class="cdm-color-swatch" data-md-color-accent="light-green" aria-pressed="false"><span class="cdm-color-swatch__dot"></span><span class="cdm-color-swatch__label">Light Green</span><span class="cdm-color-swatch__check" aria-hidden="true">&#10003;</span></button>
    <button type="button" class="cdm-color-swatch" data-md-color-accent="lime" aria-pressed="false"><span class="cdm-color-swatch__dot"></span><span class="cdm-color-swatch__label">Lime</span><span class="cdm-color-swatch__check" aria-hidden="true">&#10003;</span></button>
    <button type="button" class="cdm-color-swatch" data-md-color-accent="yellow" aria-pressed="false"><span class="cdm-color-swatch__dot"></span><span class="cdm-color-swatch__label">Yellow</span><span class="cdm-color-swatch__check" aria-hidden="true">&#10003;</span></button>
    <button type="button" class="cdm-color-swatch" data-md-color-accent="amber" aria-pressed="false"><span class="cdm-color-swatch__dot"></span><span class="cdm-color-swatch__label">Amber</span><span class="cdm-color-swatch__check" aria-hidden="true">&#10003;</span></button>
    <button type="button" class="cdm-color-swatch" data-md-color-accent="orange" aria-pressed="false"><span class="cdm-color-swatch__dot"></span><span class="cdm-color-swatch__label">Orange</span><span class="cdm-color-swatch__check" aria-hidden="true">&#10003;</span></button>
    <button type="button" class="cdm-color-swatch" data-md-color-accent="deep-orange" aria-pressed="false"><span class="cdm-color-swatch__dot"></span><span class="cdm-color-swatch__label">Deep Orange</span><span class="cdm-color-swatch__check" aria-hidden="true">&#10003;</span></button>
  </div>

  <h3>Live Preview</h3>
  <div class="cdm-palette-preview">
    <a href="#theme-color-customization" class="md-button md-button--primary">Primary Button</a>
    <span class="cdm-preview-accent-pill">Accent Highlight</span>
    <span>Hover over <a href="https://github.com/kbase/cdm-data-loaders/blob/develop/zensical.toml"><code>zensical.toml</code></a> to preview link and code hover colors.</span>
  </div>

  <p>Equivalent <code>zensical.toml</code> palette entry for the selected colors:</p>
  <pre><code id="cdm-palette-toml">[[project.theme.palette]]
primary = "teal"
accent = "teal"</code></pre>
</div>
