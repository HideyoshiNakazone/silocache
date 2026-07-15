import contextlib
import inspect
import threading
import weakref
from asyncio import Lock, get_running_loop
from collections.abc import Awaitable, Callable
from dataclasses import dataclass, field
from functools import wraps
from typing import Any, AsyncGenerator, Protocol, overload


class _NamedCallable[**P, T](Protocol):
    __name__: str

    def __call__(self, *args: P.args, **kwargs: P.kwargs) -> T: ...


class _AsyncNamedCallable[**P, T](_NamedCallable):
    __name__: str

    async def __call__(self, *args: P.args, **kwargs: P.kwargs) -> T: ...  # ty:ignore[empty-body]


class _AsyncGenNamedCallable[**P, T](_NamedCallable):
    __name__: str

    def __call__(
        self, *args: P.args, **kwargs: P.kwargs
    ) -> AsyncGenerator[T, None]: ...


@dataclass(slots=True)
class _LocalState:
    lock: Lock = field(default_factory=Lock)
    locks: dict[str, Lock] = field(default_factory=dict)
    caches: dict[str, Any] = field(default_factory=dict)
    exit_stack: contextlib.AsyncExitStack = field(
        default_factory=contextlib.AsyncExitStack
    )


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
async def _get_lock(name: str) -> AsyncGenerator[Lock, None]:
    state = _get_state()
    async with state.lock:
        if name not in state.locks:
            state.locks[name] = Lock()
    async with state.locks[name]:
        yield state.locks[name]


async def _set_cache(name: str, value: Any) -> None:
    state = _get_state()
    async with _get_lock(name):
        state.caches[name] = value


async def _get_cache(name: str) -> Any:
    state = _get_state()
    async with _get_lock(name):
        return state.caches.get(name)


async def _run_factory[**P, T](
    state: _LocalState,
    factory: _NamedCallable[P, T]
    | _AsyncNamedCallable[P, T]
    | _AsyncGenNamedCallable[P, T],
    *args: P.args,
    **kwargs: P.kwargs,
) -> T:
    if inspect.isasyncgenfunction(factory):
        context = contextlib.asynccontextmanager(factory)(*args, **kwargs)
        return await state.exit_stack.enter_async_context(context)

    if inspect.iscoroutinefunction(factory):
        return await factory(*args, **kwargs)

    value = factory(*args, **kwargs)
    # Only factories built on an async generator (e.g. @asynccontextmanager) are
    # entered; a value that merely supports `async with` (a lock, a client) is
    # cached as-is.
    if inspect.isasyncgenfunction(inspect.unwrap(factory)) and isinstance(
        value, contextlib.AbstractAsyncContextManager
    ):
        return await state.exit_stack.enter_async_context(value)
    return value


async def _get_or_set_cache[**P, T](
    name: str,
    factory: _NamedCallable[P, T]
    | _AsyncNamedCallable[P, T]
    | _AsyncGenNamedCallable[P, T],
    *args: P.args,
    **kwargs: P.kwargs,
) -> T:
    state = _get_state()
    async with _get_lock(name):
        if name not in state.caches:
            state.caches[name] = await _run_factory(state, factory, *args, **kwargs)
        return state.caches[name]


async def flushall_singleton_cache() -> None:
    """Flush the singleton cache for the current thread + event loop.

    Cleanup of async generator and context manager factories runs here,
    in reverse creation order.
    """
    state = _get_state()
    async with state.lock:
        state.caches.clear()
        state.locks.clear()
        exit_stack = state.exit_stack
        state.exit_stack = contextlib.AsyncExitStack()
    # Closed outside state.lock so cleanup code can itself await cached factories.
    await exit_stack.aclose()


class _SingletonDecorator(Protocol):
    @overload
    def __call__[**P, T](
        self, func: _AsyncGenNamedCallable[P, T]
    ) -> Callable[P, Awaitable[T]]: ...

    @overload
    def __call__[**P, T](
        self, func: _NamedCallable[P, contextlib.AbstractAsyncContextManager[T]]
    ) -> Callable[P, Awaitable[T]]: ...

    @overload
    def __call__[**P, T](
        self, func: _NamedCallable[P, T] | _AsyncNamedCallable[P, T]
    ) -> Callable[P, Awaitable[T]]: ...


@overload
def cache_singleton[**P, T](
    arg: _AsyncGenNamedCallable[P, T],
) -> Callable[P, Awaitable[T]]: ...


@overload
def cache_singleton[**P, T](
    arg: _NamedCallable[P, contextlib.AbstractAsyncContextManager[T]],
) -> Callable[P, Awaitable[T]]: ...


@overload
def cache_singleton[**P, T](
    arg: _NamedCallable[P, T] | _AsyncNamedCallable[P, T],
) -> Callable[P, Awaitable[T]]: ...


@overload
def cache_singleton(arg: str) -> _SingletonDecorator: ...


def cache_singleton(
    arg: _NamedCallable | _AsyncNamedCallable | _AsyncGenNamedCallable | str,
) -> Callable[..., Awaitable[Any]] | _SingletonDecorator:
    """A decorator to cache a singleton instance of a class or function result,
    scoped to the current thread and event loop.

    Can be used with or without a name argument:
        @cache_singleton
        def my_func(): ...

        @cache_singleton("custom_name")
        def my_func(): ...

    Factories may also be async generators (or @asynccontextmanager functions):
    the value they yield is cached, and the cleanup after the yield runs when
    flushall_singleton_cache() is called.

        @cache_singleton
        async def my_resource():
            async with open_resource() as res:
                yield res
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
