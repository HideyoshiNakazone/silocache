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
from contextlib import asynccontextmanager


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

    async def test_different_factories_initialize_concurrently(self):
        started = asyncio.Event()
        release = asyncio.Event()

        @cache_singleton
        async def blocked_factory():
            started.set()
            await release.wait()
            return object()

        @cache_singleton
        async def quick_factory():
            return object()

        blocked = asyncio.create_task(blocked_factory())
        await started.wait()

        await asyncio.wait_for(quick_factory(), timeout=1)

        release.set()
        await blocked

    async def test_nested_singleton_factories_do_not_deadlock(self):
        @cache_singleton
        async def inner():
            return object()

        @cache_singleton
        async def outer():
            return await inner()

        assert await asyncio.wait_for(outer(), timeout=1) is await inner()


class TestManagedFactories:
    async def test_async_generator_factory_caches_yielded_value(self):
        calls = 0

        @cache_singleton
        async def make_value():
            nonlocal calls
            calls += 1
            yield object()

        first = await make_value()
        second = await make_value()

        assert first is second
        assert calls == 1

    async def test_async_generator_cleanup_runs_on_flush(self):
        cleaned_up = False

        @cache_singleton
        async def make_value():
            nonlocal cleaned_up
            yield object()
            cleaned_up = True

        await make_value()
        assert not cleaned_up

        await flushall_singleton_cache()
        assert cleaned_up

    async def test_flush_reruns_async_generator_factory(self):
        @cache_singleton
        async def make_value():
            yield object()

        first = await make_value()
        await flushall_singleton_cache()
        second = await make_value()

        assert first is not second

    async def test_asynccontextmanager_factory_is_entered_once_and_exited_on_flush(
        self,
    ):
        entered = 0
        exited = 0

        @cache_singleton
        @asynccontextmanager
        async def make_value():
            nonlocal entered, exited
            entered += 1
            try:
                yield object()
            finally:
                exited += 1

        first = await make_value()
        second = await make_value()

        assert first is second
        assert entered == 1
        assert exited == 0

        await flushall_singleton_cache()
        assert exited == 1

    async def test_concurrent_async_generator_calls_invoke_factory_once(self):
        calls = 0

        @cache_singleton
        async def slow_factory():
            nonlocal calls
            calls += 1
            await asyncio.sleep(0.01)
            yield object()

        results = await asyncio.gather(*(slow_factory() for _ in range(10)))

        assert calls == 1
        assert all(result is results[0] for result in results)

    async def test_cleanup_runs_in_reverse_creation_order(self):
        order = []

        @cache_singleton
        async def first_resource():
            yield object()
            order.append("first")

        @cache_singleton
        async def second_resource():
            yield object()
            order.append("second")

        await first_resource()
        await second_resource()
        await flushall_singleton_cache()

        assert order == ["second", "first"]

    async def test_custom_name_with_async_generator_factory(self):
        @cache_singleton("shared_key")
        async def first_factory():
            yield object()

        @cache_singleton("shared_key")
        async def second_factory():
            yield object()

        assert await first_factory() is await second_factory()

    async def test_context_manager_values_from_plain_factories_are_cached_as_is(self):
        lock = asyncio.Lock()

        @cache_singleton
        def make_lock():
            return lock

        assert await make_lock() is lock
        assert not lock.locked()


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
