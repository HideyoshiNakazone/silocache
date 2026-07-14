# SiloCache

[![Test and Publish](https://github.com/HideyoshiNakazone/silocache/actions/workflows/build.yml/badge.svg)](https://github.com/HideyoshiNakazone/silocache/actions/workflows/build.yml)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](LICENSE)

**Thread/EventLoop Isolated Caching** — async-friendly singleton caching where each thread and event loop gets its own isolated cache "silo".

> [!IMPORTANT]
> **This is a personal project and does not accept contributions.** The source is public so others can read and use it. Issues are welcome, but there's no guarantee they'll be addressed, and pull requests will generally not be reviewed or merged. See [Project status](#project-status).

Unlike `functools.lru_cache` or module-level globals, SiloCache never shares state across threads or event loops. This makes it safe for applications that run multiple event loops (e.g. one per worker thread, test suites with `asyncio.run` per test, or frameworks that spin up loops behind the scenes) without any risk of leaking loop-bound objects — like database connections, HTTP clients, or `asyncio` primitives — between loops.

## Features

- **Singleton caching** via a simple decorator: the factory runs once, every later call returns the cached value.
- **Isolation by design**: caches are scoped per thread *and* per event loop. A value created in one loop is never visible from another.
- **Async-safe**: concurrent calls to the same factory are deduplicated with per-key `asyncio.Lock`s — the factory runs exactly once even under `asyncio.gather`.
- **Sync and async factories**: decorate either; the wrapped function is always awaited.
- **Custom cache keys**: share one cache entry across multiple factories, or isolate same-named ones.
- **Zero-leak cleanup**: state is held in a `WeakKeyDictionary` keyed by the running loop, so it's released when the loop is garbage-collected.
- **Fully typed** (`py.typed` included).

## Installation

```sh
pip install silocache
```

Requires Python 3.12+.

## Usage

### Basic singleton

```python
from silocache import cache_singleton


@cache_singleton
async def get_http_client():
    return httpx.AsyncClient()


client = await get_http_client()  # factory runs
same_client = await get_http_client()  # cached — factory not called again
assert client is same_client
```

Sync factories work too — the decorated function still becomes a coroutine:

```python
@cache_singleton
def get_settings():
    return Settings()


settings = await get_settings()
```

> **Note:** arguments are forwarded on the *first* call only. Once the value is cached, later calls return it regardless of arguments.


### Isolation semantics

Each thread + event loop combination has its own independent cache:

```python
@cache_singleton
def make_value():
    return object()


first = asyncio.run(make_value())
second = asyncio.run(make_value())  # new loop -> new silo -> new value
assert first is not second
```

### Flushing

Clear the cache for the *current* thread and event loop (other silos are untouched):

```python
from silocache import flushall_singleton_cache

await flushall_singleton_cache()
```

## Project status

SiloCache is something I built for my own projects and decided to make public. In practice it works more like *source-available* software than a community open source project:

- **No contributions**: pull requests are not accepted and will likely be closed without review.
- **Issues welcome, best-effort only**: feel free to report bugs or ask questions, but there's no guarantee they'll be answered or fixed.
- **No support or roadmap**: things change when my own projects need them to; there are no guarantees about releases, backwards compatibility, or fixes.
- **Use freely**: the code is [MIT licensed](LICENSE), so you're welcome to use it, vendor it, or fork it and take it in your own direction.

## Development

These notes are for my own reference. The project uses [uv](https://docs.astral.sh/uv/) and [poethepoet](https://poethepoet.natn.io/):

```sh
uv sync --all-extras --dev  # install dependencies
uv run poe create-hooks     # set up git hooks
uv run poe tests            # run the test suite with coverage
```

## License

[MIT](LICENSE)