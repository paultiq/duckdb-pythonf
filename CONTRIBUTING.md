# Contributing to duckdb-python

## General Guidelines

### **Did you find a bug?**

* **Ensure the bug was not already reported** by searching on GitHub under [Issues](https://github.com/duckdb/duckdb-python/issues).
* If you're unable to find an open issue addressing the problem, [open a new one](https://github.com/duckdb/duckdb-python/issues/new/choose). Be sure to include a **title and clear description**, as much relevant information as possible, and a **code sample** or an **executable test case** demonstrating the expected behavior that is not occurring.

### **Did you write a patch that fixes a bug?**

* Great!
* If possible, add a unit test case to make sure the issue does not occur again.
* Open a new GitHub pull request with the patch.
* Ensure the PR description clearly describes the problem and solution. Include the relevant issue number if applicable.

### Outside Contributors

* Discuss your intended changes with the core team on Github
* Announce that you are working or want to work on a specific issue
* Avoid large pull requests - they are much less likely to be merged as they are incredibly hard to review

### Pull Requests

* Do not commit/push directly to the main branch. Instead, create a fork and file a pull request.
* When maintaining a branch, merge frequently with the main.
* When maintaining a branch, submit pull requests to the main frequently.
* If you are working on a bigger issue try to split it up into several smaller issues.
* Please do not open "Draft" pull requests. Rather, use issues or discussion topics to discuss whatever needs discussing.
* We reserve full and final discretion over whether or not we will merge a pull request. Adhering to these guidelines is not a complete guarantee that your pull request will be merged.

### CI for pull requests

* Pull requests will need to pass all continuous integration checks before merging.
* For faster iteration and more control, consider running CI on your own fork or when possible directly locally.
* Submitting changes to an open pull request will move it to 'draft' state.
* Pull requests will get a complete run on the main repo CI only when marked as 'ready for review' (via Web UI, button on bottom right).

### Testing cross-platform and cross-Python

* On your fork you can [run](https://docs.github.com/en/actions/using-workflows/manually-running-a-workflow#running-a-workflow) the Packaging workflow manually for any branch. You can choose whether to build for all platforms or a subset, and to either run the full testsuite, the fast tests only, or no tests at all.

## Setting up a development environment

Start by [forking duckdb-python](https://github.com/duckdb/duckdb-python/fork) into
a personal repository.

After forking the duckdb-python repo we recommend you clone your fork as follows:
```shell
git clone --recurse-submodules $REPO_URL
git remote add upstream https://github.com/duckdb/duckdb-python.git
git fetch --all
```

... or, if you have already cloned your fork:
```shell
git submodule update --init --recursive
git remote add upstream https://github.com/duckdb/duckdb-python.git
git fetch --all
```

Two things to be aware of when cloning this repository:
* DuckDB is vendored as a git submodule and needs to be initialized during or after cloning duckdb-python.
* Currently, for DuckDB to determine its version while building, it depends on the local availability of its tags.

After forking the duckdb-python repo we recommend you clone your fork as follows:

### Submodule update hook

If you'll be switching between branches that are have the submodule set to different refs, then make your life 
easier and add the git hooks in the .githooks directory to your local config: 
```shell
git config --local core.hooksPath .githooks/
```

### Editable installs (general)

It's good to be aware of the following when performing an editable install:

- `uv sync` or `uv run [tool]` perform an editable install by default. We have 
  configured the project so that scikit-build-core will use a persistent build-dir, but since the build itself 
  happens in an isolated, ephemeral environment, cmake's paths will point to non-existing directories. CMake itself 
  will be missing.
- You should install all development dependencies, and then build the project without build isolation, in two separate 
  steps. After this you can happily keep building and running, as long as you don't forget to pass in the 
  `--no-build-isolation` flag.

```bash
# install all dev dependencies without building the project (needed once)
uv sync -p 3.11 --no-install-project
# build and install without build isolation
uv sync --no-build-isolation
```

### Editable installs (IDEs)

If you're using an IDE then life is a little simpler. You install build dependencies and the project in the two 
steps outlined above, and from that point on you can rely on e.g. CLion's cmake capabilities to do incremental 
compilation and editable rebuilds. This will skip scikit-build-core's build backend and all of uv's dependency 
management, so for "real" builds you better revert to the CLI. However, this should work fine for coding and debugging.

## Day to day development

After setting up the development environment, these are the most common tasks you'll be performing.

### Tooling
This codebase is developed with the following tools:
- [Astral uv](https://docs.astral.sh/uv/) - for dependency management across all platforms we provide wheels for,
  and for Python environment management. It will be hard to work on this codebase without having UV installed.
- [Scikit-build-core](https://scikit-build-core.readthedocs.io/en/latest/index.html) - the build backend for
  building the extension. On the background, scikit-build-core uses cmake and ninja for compilation.
- [pybind11](https://pybind11.readthedocs.io/en/stable/index.html) - a bridge between C++ and Python.
- [CMake](https://cmake.org/) - the build system for both DuckDB itself and the DuckDB Python module.
- Cibuildwheel

### Cleaning
```shell
uv cache clean
rm -rf build .venv uv.lock
```

### Running tests

  Run all pytests:
```bash
uv run --no-build-isolation pytest ./tests --verbose
```

  Exclude the test/slow directory:
```bash
uv run --no-build-isolation pytest ./tests --verbose --ignore=./tests/slow
```

### Test coverage

  Run with coverage (during development you probably want to specify which tests to run):
```bash
COVERAGE=1 uv run --no-build-isolation coverage run -m pytest ./tests --verbose
```

  The `COVERAGE` env var will compile the extension with `--coverage`, allowing us to collect coverage stats of C++ 
  code as well as Python code.

  Check coverage for Python code:
```bash
uvx coverage html -d htmlcov-python
uvx coverage report --format=markdown
```

  Check coverage for C++ code (note: this will clutter your project dir with html files, consider saving them in some 
  other place):
```bash
uvx gcovr \
  --gcov-ignore-errors all \
  --root "$PWD" \
  --filter "${PWD}/src/duckdb_py" \
  --exclude '.*/\.cache/.*' \
  --gcov-exclude '.*/\.cache/.*' \
  --gcov-exclude '.*/external/.*' \
  --gcov-exclude '.*/site-packages/.*' \
  --exclude-unreachable-branches \
  --exclude-throw-branches \
  --html --html-details -o coverage-cpp.html \
  build/coverage/src/duckdb_py \
  --print-summary
```

### Typechecking, linting, style, and formatting

- We're not running any mypy typechecking tests at the moment
- We're not running any Ruff / linting / formatting at the moment
- Follow the [Google Python styleguide](https://google.github.io/styleguide/pyguide.html)
- See the section on [Comments and Docstrings](https://google.github.io/styleguide/pyguide.html#s3.8-comments-and-docstrings)

### Building wheels and sdists

To build a wheel and sdist for your system and the default Python version:
```bash
uv build
````

To build a wheel for a different Python version:
```bash
# E.g. for Python 3.9
uv build -p 3.9
```

### Cibuildwheel

You can run cibuildwheel locally for Linux. E.g. limited to Python 3.9:
```bash
CIBW_BUILD='cp39-*' uvx cibuildwheel --platform linux .
```

### Merging changes to pythonpkg from duckdb main

1. Checkout main
2Identify the merge commits that brought in tags to main:
```bash
git log --graph --oneline --decorate main --simplify-by-decoration
```

3. Get the log of commits
```bash
git log --oneline 71c5c07cdd..c9254ecff2 -- tools/pythonpkg/
```

4. Checkout v1.3-ossivalis
5. Get the log of commits
```bash
git log --oneline v1.3.0..v1.3.1 -- tools/pythonpkg/
```
git diff --name-status 71c5c07cdd c9254ecff2 -- tools/pythonpkg/

```bash
git log --oneline 71c5c07cdd..c9254ecff2 -- tools/pythonpkg/
git diff --name-status <HASH_A> <HASH_B> -- tools/pythonpkg/
```

## Versioning and Releases

The DuckDB Python package versioning and release scheme follows that of DuckDB itself. This means that a `X.Y.Z[.
postN]` release of the Python package ships the DuckDB stable release `X.Y.Z`. The optional `.postN` releases ship the same stable release of DuckDB as their predecessors plus Python package-specific fixes and / or features.

| Types                                                                  | DuckDB Version | Resulting Python Extension Version |
|------------------------------------------------------------------------|----------------|------------------------------------|
| Stable release: DuckDB stable release                                  | `1.3.1`        | `1.3.1`                            |
| Stable post release: DuckDB stable release + Python fixes and features | `1.3.1`        | `1.3.1.postX`                      |
| Nightly micro: DuckDB next micro nightly + Python next micro nightly   | `1.3.2.devM`   | `1.3.2.devN`                       |
| Nightly minor: DuckDB next minor nightly + Python next minor nightly   | `1.4.0.devM`   | `1.4.0.devN`                       |

Note that we do not ship nightly post releases (e.g. we don't ship `1.3.1.post2.dev3`).

### Branch and Tag Strategy

We cut releases as follows:

| Type                 | Tag          | How                                                                             |
|----------------------|--------------|---------------------------------------------------------------------------------|
| Stable minor release | vX.Y.0       | Adding a tag on `main`                                                          |
| Stable micro release | vX.Y.Z       | Adding a tag on a minor release branch (e.g. `v1.3-ossivalis`)                  |
| Stable post release  | vX.Y.Z-postN | Adding a tag on a post release branch (e.g. `v1.3.1-post`)                      |
| Nightly micro        | _not tagged_ | Combining HEAD of the _micro_ release branches of DuckDB and the Python package |
| Nightly minor        | _not tagged_ | Combining HEAD of the _minor_ release branches of DuckDB and the Python package |

### Release Runbooks

We cut a new **stable minor release** with the following steps:
1. Create a PR on `main` to pin the DuckDB submodule to the tag of its current release.
1. Iff all tests pass in CI, merge the PR.
1. Manually start the release workflow with the hash of this commit, and the tag name.
1. Iff all goes well, create a new PR to let the submodule track DuckDB main.

We cut a new **stable micro release** with the following steps:
1. Create a PR on the minor release branch to pin the DuckDB submodule to the tag of its current release.
1. Iff all tests pass in CI, merge the PR.
1. Manually start the release workflow with the hash of this commit, and the tag name.
1. Iff all goes well, create a new PR to let the submodule track DuckDB's minor release branch.

We cut a new **stable post release** with the following steps:
1. Create a PR on the post release branch to pin the DuckDB submodule to the tag of its current release.
1. Iff all tests pass in CI, merge the PR.
1. Manually start the release workflow with the hash of this commit, and the tag name.
1. Iff all goes well, create a new PR to let the submodule track DuckDB's minor release branch.

### Dynamic Versioning Integration

The package uses `setuptools_scm` with `scikit-build` for automatic version determination, and implements a custom
versioning scheme.

- **pyproject.toml configuration**:
  ```toml
  [tool.scikit-build]
  metadata.version.provider = "scikit_build_core.metadata.setuptools_scm"
  
  [tool.setuptools_scm]
  version_scheme = "duckdb_packaging._setuptools_scm_version:version_scheme"
  ```

- **Environment variables**:
  - `MAIN_BRANCH_VERSIONING=0`: Use release branch versioning (patch increments)
  - `MAIN_BRANCH_VERSIONING=1`: Use main branch versioning (minor increments)
  - `OVERRIDE_GIT_DESCRIBE`: Override version detection