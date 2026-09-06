# Contributing

Thanks for contributing to Trakkin. Keep changes focused, follow existing patterns, and include validation that covers your change.

## Before You Start

- Search [existing issues](https://github.com/trakkin/trakkin/issues) before opening a new one or starting substantial work.
- Use the [issue chooser](https://github.com/trakkin/trakkin/issues/new/choose) for bugs and feature proposals.
- Open an issue before starting a large feature, provider contract change, or architectural redesign.
- Report security vulnerabilities privately as described in [`SECURITY.md`](SECURITY.md).

## Development Setup

The repository uses [mise](https://mise.jdx.dev/) to pin development tools and provide a common task interface. See [`mise.toml`](mise.toml) for the complete definitions.

Set up the repository with:

```sh
mise run setup
```

Start the server and web development processes with:

```sh
mise run dev
```

VS Code users can optionally use the repository's Dev Container configuration.

Run `mise run --list` to see all available tasks.

## Tasks

The main repository tasks are:

| Task              | Purpose                                                              |
| ----------------- | -------------------------------------------------------------------- |
| `mise run setup`  | Install project dependencies and prepare the development environment |
| `mise run dev`    | Run the server and web development processes with hot reloading      |
| `mise run check`  | Check workflows, server, web, documentation, and SQL sources         |
| `mise run format` | Format server, web, and documentation sources                        |
| `mise run fix`    | Apply automated server, web, documentation, and SQL fixes            |
| `mise run test`   | Run server and web tests                                             |
| `mise run build`  | Build the release executable to `dist/`                              |
| `mise run clean`  | Remove build artifacts                                               |

Target a specific subsystem with scope-first tasks such as `mise run server:check`, `mise run web:test`, and `mise run docs:build`. See [`mise.toml`](mise.toml) or run `mise run --list` for the complete list.

## Tooling

The repository uses these tools for formatting, linting, testing, and development:

- **Rust:** `rustfmt` for formatting, Clippy for linting, and Cargo's test runner for tests.
- **Frontend:** Biome and the frontend package scripts for formatting and linting.
- **SQL:** `cargo-dylint` and sqruff.
- **GitHub Actions:** actionlint.
- **JavaScript tooling:** Node.js and pnpm.
- **Browser dependencies:** Playwright with Chromium.
- **Development processes:** watchexec restarts development processes when relevant files change.

Tool versions are pinned in [`mise.toml`](mise.toml).

## Tests

Add or update tests for behavior changes. If a relevant test cannot reasonably be added, explain why in the pull request.

For Rust:

- Keep localized/unit tests beside the owning module in a `#[cfg(test)] mod tests`.
- Keep multi-module behavior tests under `tests/`.
- Name tests for the subject, condition, and expected outcome.

## Validation

Run the checks that cover your change while iterating:

- Server: `mise run server:check` and `mise run server:test`
- Web: `mise run web:check`, `mise run web:test`, and `mise run web:build`
- Documentation: `mise run docs:check` and `mise run docs:build`
- SQL: `mise run sql:check`

## Branches

Open pull requests against the `next` branch. Only consider opening pull requests against `main` if there is an urgent stable fix.

## Pull Requests

- Keep each pull request focused on one coherent change.
- Use a draft pull request while the change is incomplete.
- Avoid unrelated formatting or cleanup.
- Use `Closes #123` when merging should close an issue, or `Refs #123` when it should only reference one.
- Describe the motivation and user-visible behavior.
- Include screenshots or recordings for visible frontend changes if you can.
- Update documentation when configuration, contracts, or migrations change.
- Format the pull request title as a Conventional Commit.

## Commits

Use [Conventional Commits](https://www.conventionalcommits.org/en/v1.0.0/).

For example:

```text
fix: handle unavailable source
feat(example): support example provider
refactor(sync): simplify run planning
```

## Changelog

Do not manually add entries to [`CHANGELOG.md`](CHANGELOG.md). Release Please generates them automatically from Conventional Commit history.

## License

By contributing, you agree that your contribution is licensed under the repository's [`LICENSE`](LICENSE).
