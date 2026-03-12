# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/)
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

---

## [Unreleased]

*(Add upcoming changes here before tagging a new release.)*

---

## [0.1.0] - 2026-03-12

### Added
- Initial public release of `mibel-simulator`.
- Full MIBEL Iberian day-ahead market clearing model implemented as a Pyomo
  mixed-integer linear programme (MILP).
- `run_mibel_simulator()` — high-level entry point that parses raw OMIE files
  and returns a results dictionary containing clearing prices, cleared bids,
  Spain-Portugal transmissions and full iteration history.
- `run_iterative_loop()` — iterative SCO/MIC paradox-order resolution
  algorithm with optional parallel execution (`n_jobs`).
- `run_model()` — single-iteration Pyomo model builder and solver wrapper.
- Configurable solver via `solver_factory_type` argument (default: `"gurobi"`).
  Any Pyomo-compatible solver (e.g. CBC, GLPK) can be used.
- Per-solver option compatibility filtering: incompatible options are skipped
  with a `UserWarning` instead of crashing.
- Descriptive `RuntimeError` with install instructions when a solver binary is
  missing from PATH.
- `solver_options` dictionary argument forwarded through the full call chain.
- File parsers: `parse_cab_file`, `parse_det_file`, `parse_capacidad_inter_file`.
- Residual demand curve utilities: `calculate_residual_demand_curves`,
  `interpolate_residual_demand_curves`.
- `pandera` schema validation on all public DataFrame arguments.
- Bundled CSV data files (demand curves) distributed with the package.
- Packaging: `pyproject.toml`-based build (setuptools ≥ 68), dynamic version
  from `mibel_simulator.__version__`, MIT licence.
- CI: GitHub Actions workflow for unit tests (Python 3.10 / 3.11 / 3.12),
  static analysis (ruff + mypy), and tag-triggered PyPI publish via Trusted
  Publishing.
- Test suite: unit tests for solver factory configuration and option filtering.
- Integration smoke test: full `run_iterative_loop()` round-trip using CBC.

---

## Versioning policy

This project follows [Semantic Versioning 2.0.0](https://semver.org):

| Change type                       | Version bump                  | Example                    |
| --------------------------------- | ----------------------------- | -------------------------- |
| Backwards-incompatible API change | **MAJOR** (`X.y.z → X+1.0.0`) | removing a public argument |
| New backwards-compatible feature  | **MINOR** (`x.Y.z → x.Y+1.0`) | new optional argument      |
| Bug fix / internal improvement    | **PATCH** (`x.y.Z → x.y.Z+1`) | fix wrong clearing price   |

### Tagging and releasing

1. Update `__version__` in `src/mibel_simulator/__init__.py`.
2. Add a dated section to this file under `## [X.Y.Z] - YYYY-MM-DD`.
3. Commit and push: `git commit -m "chore: release vX.Y.Z"`.
4. Create and push a tag: `git tag vX.Y.Z && git push origin vX.Y.Z`.
5. The `publish.yml` GitHub Actions workflow will automatically build and
   upload the distribution to PyPI via Trusted Publishing.

[Unreleased]: https://github.com/EloyID/mibel-simulator/compare/v0.1.0...HEAD
[0.1.0]: https://github.com/EloyID/mibel-simulator/releases/tag/v0.1.0
