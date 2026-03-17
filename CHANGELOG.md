# Changelog

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
