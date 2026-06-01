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

Start the backend and frontend development servers with:

```sh
mise run dev
```

VS Code users can optionally use the repository's Dev Container configuration.

Run `mise run --list` to see all available tasks.

## Tasks

The main repository tasks are:

| Task               | Purpose                                                              |
| ------------------ | -------------------------------------------------------------------- |
| `mise run setup`   | Install project dependencies and prepare the development environment |
| `mise run dev`     | Run the backend and frontend development servers with hot reloading  |
| `mise run lint`    | Run application linters                                              |
| `mise run format`  | Format application source files                                      |
| `mise run fix`     | Apply automatically fixable lint and formatting changes              |
| `mise run test`    | Run backend and frontend tests                                       |
| `mise run build`   | Build the release executable to `dist/`                              |
| `mise run clean`   | Remove build artifacts                                               |
| `mise run prepare` | Prepare frontend browser dependencies                                |

There are also sub-tasks for each subsystem, such as `mise run lint:backend`, `mise run test:frontend`, and `mise run build:docs`. See [`mise.toml`](mise.toml) or run `mise run --list` for the complete list.

## Tooling

The repository uses these tools for formatting, linting, testing, and development:

- **Rust:** `rustfmt` for formatting, Clippy for linting, and Cargo's test runner for tests.
- **Frontend:** Biome and the frontend package scripts for formatting and linting.
- **Protobuf:** Buf for linting and formatting.
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

- Backend: `mise run lint:backend` and `mise run test:backend`
- Frontend: `mise run lint:frontend`, `mise run test:frontend`, and `mise run build:frontend`
- Documentation: `mise run lint:docs` and `mise run build:docs`
- Protobuf: `mise run lint:proto`
- SQL: `mise run lint:sql`

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
