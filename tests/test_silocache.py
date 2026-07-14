from silocache import (
    _get_cache,
    _get_lock,
    _get_state,
    _set_cache,
    cache_singleton,
    flushall_singleton_cache,
)

import asyncio
import threading
from concurrent.futures import ThreadPoolExecutor


class TestCacheSingleton:
    async def test_sync_factory_is_called_once(self):
        calls = 0

        @cache_singleton
        def make_value():
            nonlocal calls
            calls += 1
            return object()

        first = await make_value()
        second = await make_value()

        assert first is second
        assert calls == 1

    async def test_async_factory_is_called_once(self):
        calls = 0

        @cache_singleton
        async def make_value():
            nonlocal calls
            calls += 1
            return object()

        first = await make_value()
        second = await make_value()

        assert first is second
        assert calls == 1

    async def test_arguments_are_forwarded_on_first_call(self):
        @cache_singleton
        def make_value(a, b=0):
            return a + b

        assert await make_value(1, b=2) == 3

    async def test_arguments_are_ignored_once_cached(self):
        @cache_singleton
        def make_value(a):
            return a

        assert await make_value(1) == 1
        assert await make_value(99) == 1

    async def test_none_result_is_cached(self):
        calls = 0

        @cache_singleton
        def make_value():
            nonlocal calls
            calls += 1
            return None

        assert await make_value() is None
        assert await make_value() is None
        assert calls == 1

    async def test_wrapper_preserves_function_metadata(self):
        @cache_singleton
        def my_factory():
            """My docstring."""

        assert my_factory.__name__ == "my_factory"
        assert my_factory.__doc__ == "My docstring."

    async def test_cache_is_keyed_by_function_name(self):
        def make():
            @cache_singleton
            def shared_name():
                return object()

            return shared_name

        first_func, second_func = make(), make()
        assert await first_func() is await second_func()

    async def test_custom_name_factory_is_called_once(self):
        calls = 0

        @cache_singleton("custom_name")
        def make_value():
            nonlocal calls
            calls += 1
            return object()

        first = await make_value()
        second = await make_value()

        assert first is second
        assert calls == 1

    async def test_custom_name_with_async_factory(self):
        calls = 0

        @cache_singleton("custom_name")
        async def make_value():
            nonlocal calls
            calls += 1
            return object()

        first = await make_value()
        second = await make_value()

        assert first is second
        assert calls == 1

    async def test_custom_name_overrides_function_name_as_cache_key(self):
        @cache_singleton("shared_key")
        def first_factory():
            return object()

        @cache_singleton("shared_key")
        def second_factory():
            return object()

        assert await first_factory() is await second_factory()

    async def test_custom_name_isolates_functions_with_same_name(self):
        def make(name):
            @cache_singleton(name)
            def shared_name():
                return object()

            return shared_name

        first_func, second_func = make("first_key"), make("second_key")
        assert await first_func() is not await second_func()

    async def test_custom_name_is_used_as_cache_entry(self):
        value = object()

        @cache_singleton("entry_key")
        def make_value():
            return value

        await make_value()
        assert await _get_cache("entry_key") is value
        assert await _get_cache("make_value") is None

    async def test_custom_name_wrapper_preserves_function_metadata(self):
        @cache_singleton("custom_name")
        def my_factory():
            """My docstring."""

        assert my_factory.__name__ == "my_factory"
        assert my_factory.__doc__ == "My docstring."

    async def test_concurrent_calls_only_invoke_factory_once(self):
        calls = 0

        @cache_singleton
        async def slow_factory():
            nonlocal calls
            calls += 1
            await asyncio.sleep(0.01)
            return object()

        results = await asyncio.gather(*(slow_factory() for _ in range(10)))

        assert calls == 1
        assert all(result is results[0] for result in results)


class TestFlushallSingletonCache:
    async def test_flush_forces_factory_to_run_again(self):
        calls = 0

        @cache_singleton
        def make_value():
            nonlocal calls
            calls += 1
            return object()

        first = await make_value()
        await flushall_singleton_cache()
        second = await make_value()

        assert calls == 2
        assert first is not second

    async def test_flush_clears_named_locks(self):
        @cache_singleton
        def make_value():
            return object()

        await make_value()
        state = _get_state()
        assert state.caches and state.locks

        await flushall_singleton_cache()
        assert not state.caches
        assert not state.locks


class TestIsolation:
    async def test_state_is_stable_within_a_loop(self):
        assert _get_state() is _get_state()

    def test_each_event_loop_gets_its_own_cache(self):
        @cache_singleton
        def make_value():
            return object()

        first = asyncio.run(make_value())
        second = asyncio.run(make_value())

        assert first is not second

    def test_each_thread_gets_its_own_cache(self):
        @cache_singleton
        async def make_value():
            await asyncio.sleep(0.01)
            return object()

        with ThreadPoolExecutor(max_workers=2) as pool:
            futures = [pool.submit(asyncio.run, make_value()) for _ in range(2)]
            first, second = [future.result() for future in futures]

        assert first is not second

    def test_flush_only_affects_current_thread(self):
        @cache_singleton
        def make_value():
            return object()

        async def fill_then_wait(filled: threading.Event, flushed: threading.Event):
            value = await make_value()
            filled.set()
            await asyncio.to_thread(flushed.wait, 5)
            return value, await make_value()

        async def flush(filled: threading.Event, flushed: threading.Event):
            await asyncio.to_thread(filled.wait, 5)
            await make_value()
            await flushall_singleton_cache()
            flushed.set()

        filled, flushed = threading.Event(), threading.Event()
        with ThreadPoolExecutor(max_workers=2) as pool:
            survivor = pool.submit(asyncio.run, fill_then_wait(filled, flushed))
            pool.submit(asyncio.run, flush(filled, flushed)).result()
            before_flush, after_flush = survivor.result()

        assert before_flush is after_flush


class TestCacheAccess:
    async def test_set_then_get_round_trips(self):
        value = object()
        await _set_cache("entry", value)
        assert await _get_cache("entry") is value

    async def test_get_missing_entry_returns_none(self):
        assert await _get_cache("missing") is None


class TestGetLock:
    async def test_same_name_yields_same_lock(self):
        async with _get_lock("resource") as first:
            pass
        async with _get_lock("resource") as second:
            pass

        assert first is second

    async def test_different_names_yield_different_locks(self):
        async with _get_lock("first") as first:
            pass
        async with _get_lock("second") as second:
            pass

        assert first is not second
