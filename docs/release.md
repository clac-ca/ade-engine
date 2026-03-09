# Release process

This repo uses release-please single-package mode to automate version bumps and GitHub Releases.

## How it works

- Every push to `main` runs the Release Please workflow.
- If there are new conventional commits, it opens a release PR.
- Merging the release PR:
  - updates `pyproject.toml` and `CHANGELOG.md`
  - creates a tag like `vX.Y.Z`
  - publishes a GitHub Release

## Version source of truth

- `pyproject.toml` (`project.version`) is the canonical version.
- The workflow uses the upstream single-package action input `release-type: python`.
- The release PR keeps `pyproject.toml` in sync with the generated release version.

## Releasing

1. Merge normal PRs to `main` using conventional commit messages.
2. Wait for the `Release Please` workflow to open a release PR.
3. Merge the release PR.
4. The workflow will create the tag and GitHub Release automatically.

## Manual trigger (optional)

You can manually run the `Release Please` workflow from the Actions tab if needed.
