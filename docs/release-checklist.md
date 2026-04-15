# Release Checklist

This checklist is intended to keep `kafka-typegen` releases consistent and traceable.

## Pre-release

- confirm `main` is green on:
  - `pnpm lint`
  - `pnpm typecheck`
  - `pnpm test`
  - `pnpm build`
- run Docker-backed integration coverage in a Docker-visible environment:
  - `pnpm test:integration`
  - `pnpm test:integration:secure`
- confirm the documented stable surface still matches:
  - [stability-policy.md](./stability-policy.md)
  - README examples
  - package exports
- review `CHANGELOG.md` and move release-ready items out of `Unreleased`
- confirm npm publish workflow still:
  - publishes from pushed version tags
  - verifies the tag matches `package.json`

## Versioning

- use SemVer for all published versions
- major:
  - breaking changes to documented config, generated API defaults, CLI behavior, or documented runtime entrypoints
- minor:
  - additive features or non-breaking improvements
- patch:
  - bug fixes and documentation-only changes without public contract changes

## Release Cut

- update `package.json` version
- add the final dated section in `CHANGELOG.md`
- commit the release version and changelog
- create and push the release tag such as `v1.0.0`
- verify the published package metadata on npm

## Post-release

- verify the Git tag points at the expected commit
- verify install and import examples against the published package
- note any follow-up fixes or regressions as new issues rather than silently carrying them into the next release
