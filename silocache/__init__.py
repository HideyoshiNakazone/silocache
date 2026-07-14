import contextlib
import inspect
import threading
import weakref
from asyncio import Lock, get_running_loop
from collections.abc import Awaitable, Callable
from dataclasses import dataclass, field
from functools import wraps
from typing import Any, Protocol, overload


class _NamedCallable[**P, T](Protocol):
    __name__: str

    def __call__(self, *args: P.args, **kwargs: P.kwargs) -> T: ...


class _AsyncNamedCallable[**P, T](_NamedCallable):
    __name__: str

    async def __call__(self, *args: P.args, **kwargs: P.kwargs) -> T: ...  # ty:ignore[empty-body]


@dataclass(slots=True)
class _LocalState:
    caches: dict[str, Any] = field(default_factory=dict)
    locks: dict[str, Lock] = field(default_factory=dict)
    lock: Lock = field(default_factory=Lock)


_THREAD_LOCAL = threading.local()


def _get_state() -> _LocalState:
    loop = get_running_loop()

    try:
        per_loop: weakref.WeakKeyDictionary = _THREAD_LOCAL.per_loop
    except AttributeError:
        per_loop = weakref.WeakKeyDictionary()
        _THREAD_LOCAL.per_loop = per_loop

    state = per_loop.get(loop)
    if state is None:
        state = _LocalState()
        per_loop[loop] = state
    return state


@contextlib.asynccontextmanager
async def get_lock(name: str):
    state = _get_state()
    async with state.lock:
        if name not in state.locks:
            state.locks[name] = Lock()
        yield state.locks[name]


async def _set_cache(name: str, value: Any) -> None:
    state = _get_state()
    async with get_lock(name):
        state.caches[name] = value


async def _get_cache(name: str) -> Any:
    state = _get_state()
    async with get_lock(name):
        return state.caches.get(name)


async def _get_or_set_cache[**P, T](
    name: str,
    factory: _NamedCallable[P, T] | _AsyncNamedCallable[P, T],
    *args: P.args,
    **kwargs: P.kwargs,
) -> T:
    state = _get_state()
    async with get_lock(name):
        if name not in state.caches:
            if inspect.iscoroutinefunction(factory):
                state.caches[name] = await factory(*args, **kwargs)
            else:
                state.caches[name] = factory(*args, **kwargs)
        return state.caches[name]


async def flushall_singleton_cache() -> None:
    """Flush the singleton cache for the current thread + event loop."""
    state = _get_state()
    async with state.lock:
        state.caches.clear()
        state.locks.clear()


class _SingletonDecorator(Protocol):
    def __call__[**P, T](
        self, func: _NamedCallable[P, T] | _AsyncNamedCallable[P, T]
    ) -> Callable[P, Awaitable[T]]: ...


@overload
def cache_singleton[**P, T](
    arg: _NamedCallable[P, T] | _AsyncNamedCallable[P, T],
) -> Callable[P, Awaitable[T]]: ...


@overload
def cache_singleton(arg: str) -> _SingletonDecorator: ...


def cache_singleton(
    arg: _NamedCallable | _AsyncNamedCallable | str,
) -> Callable[..., Awaitable[Any]] | _SingletonDecorator:
    """A decorator to cache a singleton instance of a class or function result,
    scoped to the current thread and event loop.

    Can be used with or without a name argument:
        @cache_singleton
        def my_func(): ...

        @cache_singleton("custom_name")
        def my_func(): ...
    """
    if isinstance(arg, str):
        name = arg

        def decorator(factory):
            @wraps(factory)
            async def named_wrapper(*args, **kwargs):
                return await _get_or_set_cache(name, factory, *args, **kwargs)

            return named_wrapper

        return decorator

    func = arg

    @wraps(func)
    async def wrapper(*args, **kwargs):
        return await _get_or_set_cache(func.__name__, func, *args, **kwargs)

    return wrapper
