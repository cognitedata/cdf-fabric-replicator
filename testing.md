# Testing
To maintain the quality and integrity of the `cdf-fabric-replicator` codebase, we have tests utilizing the [pytest](https://docs.pytest.org/en/8.2.x/) framework that run as part of the `test` Github Action.  There are two kinds of tests:
- **Unit** - Unit tests cover a single module or function in the code and do not require any external connections to run.  Unit test files are 1 to 1 with files under `cdf-fabric-replicator`.
- **Integration** - Integration tests cover interactions between modules as part of a service and connect to CDF and Fabric as part of the test.  Integration test files are 1 to 1 with the services of the `cdf-fabric-replicator` such as the Timeseries Replicator, Fabric Extractor, etc

## Setting Up Tests
***Before running tests, ensure you have a CDF project and a Fabric workspace dedicated for testing. The integration tests create and delete CDF objects and data and require a clean environment for accurate assertions.  You risk losing data if you run these tests in a dev/production environment.***

### Environment Variables
Environment variables are not needed to run the unit tests, but are needed for the integration tests both to run locally and to run in the Github Action.

The environment variables for the tests are the same ones used for the replicator, with the addition of `TEST_CONFIG_PATH`, which is where the config file for the tests is located (in the repo, this file is under `tests/integration/test_config.yaml`).

Set your test environment variables in a `.env` file.  The integration tests will load the values from this file upon startup.

### VS Code Test Explorer
Tests can run using the test explorer in VS Code.  To set up the test explorer, ensure you have the Python extension installed in VS Code and follow the instructions under [configure tests](https://code.visualstudio.com/docs/python/testing#_configure-tests) in the VS Code docs, configuring tests using the Pytest framework.  Your test setup should look like this:

Running tests using the explorer allows you to debug tests and easily manage which tests to run.  Ensure that the tests also run using `poetry` before pushing commits.
### Poetry
`poetry` can be used to run the tests in addition to running the replicator itself.  Run all commands at the project root.  These are the important poetry commands to know:
- `poetry install` - Installs dependencies and sets configuration.  If a library is added or the coverage settings are modified this will need to be run.  `pytest` and `pytest_mock` are included in the `pyproject.toml`, so running an install will get your environment ready to run tests.
- `poetry run pytest <test_directory/>` - Runs all tests found under the test-directory path.  For example, to run just unit tests set test_directory to `tests/unit`.
- `poetry run coverage run -m pytest <test_directory/>`  - Runs the tests and creates a `.coverage` file that can be used to generate a report.
- `poetry run coverage report` - Shows the coverage report in the terminal.
A combination of the test run and coverage report is available as a VS Code launch configuration under `poetry test coverage`.

In the `pyproject.toml` file, there are configuration settings for the coverage, including the failure threshold and which files to exclude from coverage:
```
[tool.coverage.run]
omit = [".*", "*/tests/*"]
[tool.coverage.report]
fail_under = 90
```

## Github Action
The Github action runs the tests for both Python 3.10 and 3.11 using `poetry`.  This action will run for every pull request created for the repo.

***Note: Integration tests will not pass for non-members of the `cdf-fabric-replicator` repository due to repository secret access for environment variables.***
## Best Practices for Adding Tests
- If a fixture can be shared across multiple tests, add it to the `conftest.py`.  Otherwise, keep fixtures that are specific to a single test in the test file.  For example, the Cognite SDK Client belongs in `conftest.py` as all the integration tests use it to seed test data.
- Ensure that coverage is at least 90% after any code additions.  Focus especially on unit test coverage as that helps ensure that all code paths are covered and any breaking changes can be isolated to the function where they occurred.
- Individual unit tests should not take longer than a second to run, excluding test collection.  Additionally, while integration tests take longer, pay attention to increases in test run times, as that may indicate that inefficient code was introduced.
- Limit mocks to external connections and complex functions that should be covered in separate tests.  For example, mock calls to the Cognite API using the client, but don't mock simple helper functions in the code.
- For unit tests, make sure the assertions that you add for function calls are useful.  For example, ensuring that the state store was synchronized after a data write is a useful assertion, whereas asserting that a helper method was called may become stale after a refactor.
- Add integration tests when a new feature or scenario is introduced to the codebase that wasn't captured in the integration tests before.  For example, add a test if a new data type from CDF or Fabric is being replicated.  If a code addition is an expansion of an existing feature, consider if adding more parameters would cover the scenario.