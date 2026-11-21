---
name: coding-style
description: >-
  Coding style, formatting, docstring, comment, typing, and code reuse guidelines for cdm-data-loaders.
  Use when writing, modifying, refactoring, formatting, or reviewing Python code in this repository.
---

# Coding style

## Formatting

- Docstrings are in Sphinx / reST format
- All functions must have a docstring, the list of params and param type, and the return and return type.
- Constants are in all-caps and must never start with an underscore. Where possible, use immutable types or assign the type `Final` to the constant.
- Fully type-annotated code (ruff ANN); datetime values carry an explicit timezone (DTZ); use Decimal for exact arithmetic, never constructed from floats.
- Do not add `__all__` blocks.


## Conventions

- Use Pydantic for models unless the context requires very large numbers of objects
  - If Pydantic would have a significant negative impact on performance, use frozen, slotted dataclasses.
- Use pydantic-settings for CLI argument parsing. Settings are frozen once constructed.
- Pipelines use the pipeline structure defined in `cdm_data_loaders.pipelines.core`. When creating a new pipeline, look at the existing pipelines under `cdm_data_loaders.pipelines.jsonlines`, `cdm_data_loaders.pipelines.xmltopyarrow`, `cdm_data_loaders.pipelines.xmltodict`, or `cdm_data_loaders.pipelines.xsv` for examples of how to structure the code.


## Code and patterns reuse

- Before writing new code, search the codebase for existing functionality; if there is not an exact match, is there functionality that could be adapted?
- When using a function from the code base, look at existing usage if in doubt how it works
- Always import from the source module; do not import via another module.
- Do not make aliases solely for importing into another module; use `import <variable/function name> as <alias>` instead
- DO NOT disable ruff rules to make the checks pass. Fix the root issue instead.


## Prose

- Use "and" instead of "&"


## Code Comments

- Inline comments start with lowercase (`# resolve the schema`)
- NEVER add comments that separate code blocks (`# -- sqlglot_dialect mapping --`)
- NEVER add comments with a line of hyphens above and below, like the following:
```
# --------------
# some comment
# --------------
```
- DO NOT state the obvious
- DO NOT document your thought process
- DO NOT talk to yourself or to the coder
- Comments should ONLY refer to the current code and its usage. They should never to historic versions of the code,
sets of edits being made, or anything else that is not directly related to executing the code.


# Documentation

- The aim of standalone documentation is to assist people executing codebase functions, e.g. running scripts from the entrypoints in `pyproject.toml`.
- Do not add lists of symbols, functions, classes, errors, etc., that can be seen in the auto-generated API docs.
- Do not add diagrams of class hierarchies.
- Do not use numbered lists unless you are describing a set of steps and need to refer back to a previous step.
- Do add examples of how to use functions, as in `docs/table_transforms.md`
- Add links to the appropriate codebase files.
- Separate out user documentation from developer documentation.
  - User documentation covers executing pipelines, configuration, creating custom transforms or preprocessors, supplying schemas, and anything else about *using* the code.
  - Developer documentation includes running tests, how to create new sets of specs for a pipeline or converter, smoke testing, and anything involving codebase internals and editing the codebase. Specific details about implementations should go in comments in the codebase itself.


## Types and data structures

- Assume python 3.13+
- Always use full generic parametrization -- `dict[str, Any]` not `dict`
- All tests should return `None`
- Prefer `functools.partial` over `lambda` when currying


## Testing

- Use the `testing` skill for testing guidelines.


## Python code execution

- cdm-data-loaders uses uv to manage Python virtual environments and dependencies
- commands should be run using `uv run python ...` or `uv run pytest ...`.
- After writing code, run `uv run ruff format` for formatting.
- Run `uv run ruff check` to check for code linting issues.

## Generated files and documentation sync

- Run `uv run python -m scripts.gen_pyarrow_schema` (or `--check`) to regenerate or verify committed PyArrow `schema.py` modules from bundled XSD schemas.
- Run `uv run pytest -m docs`, `uv run python -m docs_macros`, `uv run --group docs zensical build --clean --strict`, and `uv run python -m docs_macros --clean` to verify macro-rendered CLI reference, bundled schemas, and converter/pipeline specification pages.
