---
name: testing
description: >-
  Testing conventions, structure, methodology, parametrization, fixtures, mocking, and execution rules for cdm-data-loaders.
  Use when writing, updating, debugging, or reviewing tests of any type.
---

# Testing

## Style and structure

- Module-based tests (plain `def test_*() -> None:` functions), not class-based
- Use pytest with fixtures in `conftest.py`
- Tests should be named `test_<function_name>_<conditions>`
- Tests should have a one line description indicating what the test is doing.
- Each test should return `None` and have fully typed parameters.

## Test methodology

- Before writing tests, read through the code and understand what it does. If you don't understand it or you need other files for context, ask. Do not proceed without fully understanding the context.

- Every function and class instantiation should be unit-tested. This includes:
  - Happy path
  - Every code path must be exercised
  - Every exception must be raised
  - Boundary conditions (empty strings, empty lists, None, zero, negative values, etc.)

- Pydantic classes must have all validator paths tested for success and failure conditions.

- Tests should check precise results: test that the output is exactly the data structure and values that are expected.

- If fixing a bug, write a test that reproduces the bug but expects the correct answer. The test should initially fail. Fix the bug and verify that the test passes.

- Ensure all code paths are covered by tests. Use coverage tools to identify untested code if an execution environment is available.

## Reduce test sprawl

- Do not create separate tests if all that varies is the assertions at the end of the test. Combine the assertions into a single test.

- If the same set up is used for numerous tests, check `conftest.py` files, the test module itself, and `test.helpers.*` to see if functions or fixtures already exist. If not, add new fixtures/functions in the appropriate location.

- When testing a function, do not execute that function more than once within the test _unless_ testing caching. Use `pytest.mark.parametrize` for testing the same function with different parameters. ALWAYS use human readable ids for the test cases when using `pytest.mark.parametrize`.

## Autouse Fixtures

- Autouse fixtures may be in `conftest.py` files or in the test module itself. Always check before adding new fixtures.

## Large files for test cases

- Do not keep large test cases in Python modules. Use the `data` folder and organise files according to the data source. 

## Parallel safety and isolation

- Tests should be idempotent and order-independent.

- Assume that tests may be run in any order, that they may be run multiple times, and that they may be run in parallel.

- Do not pollute the global state and avoid side effects wherever possible.

- Do not create files within the working directory; always use a temporary file or directory.

- Avoid external resources that clash with other tests.

- Tests must work on Linux, macOS and Windows. Avoid encoding paths and platform-specific code.

## Mocking

- Do not mock internal code. Only mock external dependencies (e.g. network calls, database connections, s3 bucket access).

- use pytest's tmp_path fixture to create temporary files and directories for testing.

- Calls to external HTTP resources should be recorded and replayed using `vcrpy` and `pytest-recording`.

- Use the moto library to mock AWS services.

- Integration and end-to-end tests may use multiple containers to create an appropriate working environment for fully testing code that requires services such as databases, catalogs, and s3.

## External dependencies

- Tests that contact an external service (e.g. an FTP site, etc.) should be marked with `@pytest.mark.external_request`.

- Tests with external dependencies (e.g. Spark, Ceph) should be marked (using `@pytest.mark.requires_spark`, `@pytest.mark.requires_ceph`) and skipped by default. They should only be run in a controlled environment where the external dependencies are available.

- Spark and Ceph tests are run in containers. Use the `docker-compose.lakehouse.yml` file to start the containers and run the tests inside them. Do not run these tests on the host machine.

## Running tests

- Run tests using `uv run pytest`.
