# Contributing

These rules keep pull requests small enough for a person to review.

## Pull requests

- One change per PR, opened against `main`.
- Do not stack PRs. If a PR depends on another one, set that PR's branch as the base, so the diff shows only this change. Name the dependency in the description.
- One problem per PR, as small as possible.
- Refactors and cleanups are welcome, in their own PR. Do not mix them with a fix or a feature.
- Open work in progress as a draft. A PR that is not a draft is ready for review.
- CI must be green before review.

## Descriptions

Keep them short and in plain English. Say what was broken, what the change does and how you tested it. A few sentences is enough. AI tools tend to write too much, so cut it down.

## Bug fixes

Write a test that reproduces the bug and fails first. Then write the fix. The test proves the fix solves the problem.

## AI-generated code

AI tools are fine. A person reviews every PR, so write code and descriptions for that person. The author must understand and test every change before opening the PR.

## Commits

Use `type(scope): short description`, in lowercase, without a trailing period. Example: `fix(cache): read array tags back per element`.

## Build and test

See `docs/gradlew_commands.md`. Run `./gradlew test` before opening a PR, and `./gradlew integrationTest` when the change touches the Factry Historian connection.
