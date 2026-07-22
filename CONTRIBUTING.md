# Contributing to fleetpull

Contributions are welcome and encouraged. Whether you're adding an endpoint,
fixing a bug, sharpening a docstring, or reporting a provider quirk you hit in
the field — thank you. This guide gets you set up and explains the couple of
conventions that keep the package rigorous.

## Ways to help

- **Report a bug or a provider quirk.** Open an [issue](https://github.com/andrewjordan3/fleetpull/issues)
  with what you saw and, if you can, the (sanitized) shape of the response.
  Real API responses drift; a quirk report is genuinely useful.
- **Request an endpoint.** Coverage is deliberately broad and grows endpoint by
  endpoint. If a provider surface you need isn't in [ENDPOINTS.md](ENDPOINTS.md),
  say so.
- **Send a pull request.** Fixes, new endpoints, docs, and tests are all fair
  game.

## Setup

```bash
git clone https://github.com/andrewjordan3/fleetpull
cd fleetpull
uv sync --group dev
```

(Not on `uv`? `pip install -e '.[dev]'` works too.)

## The five gates

Every change must pass all five checks, in order. These are exactly what CI
runs on your pull request, so a green local run is a green CI run:

```bash
uv run ruff format .      # formatting
uv run ruff check .       # linting
uv run mypy src/ tests/   # static types (strict)
uv run lint-imports       # import-layering contracts
uv run pytest             # the test suite (+ in-code doctests)
```

## The conventions that matter

- **Tests never hit real provider APIs.** HTTP is mocked or served from
  fixtures. Every fixture is fully synthetic — no real VINs, driver names,
  coordinates, or account identifiers.
- **New endpoints are built probe-first.** The behavior a binding encodes is
  captured from live responses, never assumed from documentation alone —
  providers ship inert documented parameters and unenforced limits. The
  step-by-step port discipline is at the bottom of [ENDPOINTS.md](ENDPOINTS.md).
- **Models mirror the wire.** Response models carry no use-case logic;
  flattening and coercion live in the records layer. One schema per
  (provider, endpoint) — no cross-endpoint or cross-provider merging.
- **The design of record is [DESIGN.md](DESIGN.md).** It carries the
  cross-module rationale — storage layout, incremental semantics, the rate
  limiter — that no single change sees. Read the relevant section before
  touching load-bearing machinery, and update it in the same change when a
  decision moves.

The full engineering standards — file organization, typing, error handling,
naming — live in [CLAUDE.md](CLAUDE.md). You don't need to memorize it; the
gates enforce most of it, and a reviewer will point you at the rest.

## Pull request checklist

- [ ] The five gates pass locally.
- [ ] New behavior has tests; new endpoints have a capture fixture and a
      drive-through.
- [ ] Docstrings and `DESIGN.md` / `ENDPOINTS.md` match what the code does.
- [ ] No real credentials, identifiers, or PII anywhere in the diff.

By contributing, you agree that your contributions are licensed under the
project's [Apache 2.0](LICENSE) license.
