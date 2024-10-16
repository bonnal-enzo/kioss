import asyncio
import datetime
import logging
import random
import time
import timeit
import unittest
from typing import (
    Any,
    Callable,
    Coroutine,
    Iterable,
    Iterator,
    List,
    Set,
    Tuple,
    Type,
    TypeVar,
    cast,
)

from parameterized import parameterized  # type: ignore

from streamable import Stream
from streamable.functions import NoopStopIteration
from streamable.util.functiontools import sidify, star

T = TypeVar("T")
R = TypeVar("R")


def timestream(stream: Stream[T], times: int = 1) -> Tuple[float, List[T]]:
    res: List[T] = []

    def iterate():
        nonlocal res
        res = list(stream)

    return timeit.timeit(iterate, number=times) / times, res


def identity_sleep(seconds: float) -> float:
    time.sleep(seconds)
    return seconds


async def async_identity_sleep(seconds: float) -> float:
    await asyncio.sleep(seconds)
    return seconds


# simulates an I/0 bound function
slow_identity_duration = 0.01


def slow_identity(x: T) -> T:
    time.sleep(slow_identity_duration)
    return x


async def async_slow_identity(x: T) -> T:
    await asyncio.sleep(slow_identity_duration)
    return x


def identity(x: T) -> T:
    return x


# fmt: off
async def async_identity(x: T) -> T: return x
# fmt: on


def square(x):
    return x**2


async def async_square(x):
    return x**2


def throw(exc: Type[Exception]):
    raise exc()


def throw_func(exc: Type[Exception]) -> Callable[[Any], None]:
    return lambda _: throw(exc)


def async_throw_func(exc: Type[Exception]) -> Callable[[Any], Coroutine]:
    async def f(_: Any) -> None:
        raise exc

    return f


def throw_for_odd_func(exc):
    return lambda i: throw(exc) if i % 2 == 1 else i


def async_throw_for_odd_func(exc):
    async def f(i):
        return throw(exc) if i % 2 == 1 else i

    return f


class TestError(Exception):
    pass


DELTA_RATE = 0.4
# size of the test collections
N = 256

src = range(N)

pair_src = range(0, N, 2)


def randomly_slowed(
    func: Callable[[T], R], min_sleep: float = 0.001, max_sleep: float = 0.05
) -> Callable[[T], R]:
    def wrap(x: T) -> R:
        time.sleep(min_sleep + random.random() * (max_sleep - min_sleep))
        return func(x)

    return wrap


def async_randomly_slowed(
    async_func: Callable[[T], Coroutine[Any, Any, R]],
    min_sleep: float = 0.001,
    max_sleep: float = 0.05,
) -> Callable[[T], Coroutine[Any, Any, R]]:
    async def wrap(x: T) -> R:
        await asyncio.sleep(min_sleep + random.random() * (max_sleep - min_sleep))
        return await async_func(x)

    return wrap


def range_raising_at_exhaustion(
    start: int, end: int, step: int, exception: Exception
) -> Iterator[int]:
    yield from range(start, end, step)
    raise exception


src_raising_at_exhaustion = lambda: range_raising_at_exhaustion(0, N, 1, TestError())


class TestStream(unittest.TestCase):
    def test_init(self) -> None:
        stream = Stream(src)
        self.assertEqual(
            stream._source,
            src,
            msg="The stream's `source` must be the source argument.",
        )
        self.assertIsNone(
            stream.upstream,
            msg="The `upstream` attribute of a base Stream's instance must be None.",
        )

        self.assertIs(
            Stream(src)
            .group(100)
            .flatten()
            .map(identity)
            .amap(async_identity)
            .filter()
            .foreach(identity)
            .aforeach(async_identity)
            .catch()
            .observe()
            .throttle(1)
            .source,
            src,
            msg="`source` must be propagated by operations",
        )

        with self.assertRaises(
            AttributeError,
            msg="attribute `source` must be read-only",
        ):
            Stream(src).source = src  # type: ignore

        with self.assertRaises(
            AttributeError,
            msg="attribute `upstream` must be read-only",
        ):
            Stream(src).upstream = Stream(src)  # type: ignore

    def test_repr_and_display(self) -> None:
        class CustomCallable:
            pass

        complex_stream: Stream[int] = (
            Stream(src)
            .truncate(1024, when=lambda _: False)
            .filter()
            .map(lambda i: (i,))
            .filter(star(bool))
            .foreach(lambda _: _)
            .aforeach(async_identity)
            .map(cast(Callable[[Any], Any], CustomCallable()))
            .amap(async_identity)
            .group(100)
            .observe("groups")
            .flatten(concurrency=4)
            .throttle(64, interval=datetime.timedelta(seconds=1))
            .observe("foos")
            .catch(TypeError, finally_raise=True)
            .catch(TypeError, replacement=None, finally_raise=True)
        )

        print(repr(complex_stream))

        explanation_1 = str(complex_stream)

        explanation_2 = str(complex_stream.map(str))
        self.assertNotEqual(
            explanation_1,
            explanation_2,
            msg="explanation of different streams must be different",
        )

        print(explanation_1)

        complex_stream.display()
        complex_stream.display(logging.ERROR)

        self.assertEqual(
            """(
    Stream(range(0, 256))
)""",
            str(Stream(src)),
            msg="`repr` should work as expected on a stream without operation",
        )
        self.assertEqual(
            """(
    Stream(range(0, 256))
    .map(<lambda>, concurrency=1, ordered=True, via='process')
)""",
            str(Stream(src).map(lambda _: _, via="process")),
            msg="`repr` should work as expected on a stream with 1 operation",
        )
        self.assertEqual(
            """(
    Stream(range(0, 256))
    .truncate(count=1024, when=<lambda>)
    .filter(bool)
    .map(<lambda>, concurrency=1, ordered=True, via='thread')
    .filter(star(bool))
    .foreach(<lambda>, concurrency=1, ordered=True, via='thread')
    .aforeach(async_identity, concurrency=1, ordered=True)
    .map(CustomCallable(...), concurrency=1, ordered=True, via='thread')
    .amap(async_identity, concurrency=1, ordered=True)
    .group(size=100, by=None, interval=None)
    .observe('groups')
    .flatten(concurrency=4)
    .throttle(per_second=64, per_minute=inf, per_hour=inf, interval=datetime.timedelta(seconds=1))
    .observe('foos')
    .catch(TypeError, when=bool, finally_raise=True)
    .catch(TypeError, when=bool, replacement=None, finally_raise=True)
)""",
            str(complex_stream),
            msg="`repr` should work as expected on a stream with many operation",
        )

    def test_iter(self) -> None:
        self.assertIsInstance(
            iter(Stream(src)),
            Iterator,
            msg="iter(stream) must return an Iterator.",
        )

        with self.assertRaisesRegex(
            TypeError,
            "`source` must be either a Callable\[\[\], Iterable\] or an Iterable, but got a int",
            msg="Getting an Iterator from a Stream with a source not being a Union[Callable[[], Iterator], ITerable] must raise TypeError.",
        ):
            iter(Stream(1))  # type: ignore

        with self.assertRaisesRegex(
            TypeError,
            "`source` must be either a Callable\[\[\], Iterable\] or an Iterable, but got a Callable\[\[\], int\]",
            msg="Getting an Iterator from a Stream with a source not being a Union[Callable[[], Iterator], ITerable] must raise TypeError.",
        ):
            iter(Stream(lambda: 1))  # type: ignore

    def test_add(self) -> None:
        from streamable.stream import FlattenStream

        stream = Stream(src)
        self.assertIsInstance(
            stream + stream,
            FlattenStream,
            msg="stream addition must return a FlattenStream.",
        )

        stream_a = Stream(range(10))
        stream_b = Stream(range(10, 20))
        stream_c = Stream(range(20, 30))
        self.assertListEqual(
            list(stream_a + stream_b + stream_c),
            list(range(30)),
            msg="`chain` must yield the elements of the first stream the move on with the elements of the next ones and so on.",
        )

    @parameterized.expand(
        [
            [Stream.map, [identity]],
            [Stream.amap, [async_identity]],
            [Stream.foreach, [identity]],
            [Stream.flatten, []],
        ]
    )
    def test_sanitize_concurrency(self, method, args) -> None:
        stream = Stream(src)
        with self.assertRaises(
            TypeError,
            msg=f"`{method}` should be raising TypeError for non-int concurrency.",
        ):
            method(stream, *args, concurrency="1")

        with self.assertRaises(
            ValueError,
            msg=f"`{method}` should be raising ValueError for concurrency=0.",
        ):
            method(stream, *args, concurrency=0)

        for concurrency in range(1, 10):
            self.assertIsInstance(
                method(stream, *args, concurrency=concurrency),
                Stream,
                msg=f"It must be ok to call {method} with concurrency={concurrency}.",
            )

    @parameterized.expand(
        [
            (Stream.map,),
            (Stream.foreach,),
        ]
    )
    def test_sanitize_via(self, method) -> None:
        with self.assertRaisesRegex(
            TypeError,
            "`via` should be 'thread' or 'process', but got 'foo'.",
            msg=f"`{method}` must raise a TypeError for invalid via",
        ):
            method(Stream(src), identity, via="foo")

    @parameterized.expand(
        [
            [1],
            [2],
        ]
    )
    def test_map(self, concurrency) -> None:
        self.assertListEqual(
            list(Stream(src).map(randomly_slowed(square), concurrency=concurrency)),
            list(map(square, src)),
            msg="At any concurrency the `map` method should act as the builtin map function, transforming elements while preserving input elements order.",
        )

    @parameterized.expand(
        [
            [True, identity],
            [False, sorted],
        ]
    )
    def test_process_concurrency(
        self, ordered, order_mutation
    ) -> None:  # pragma: no cover
        import sys

        if sys.version < "3.9.0":
            return

        lambda_identity = lambda x: x * 10

        def local_identity(x):
            return x

        for f in [lambda_identity, local_identity]:
            with self.assertRaisesRegex(
                AttributeError,
                "Can't pickle",
                msg="process-based concurrency should not be able to serialize a lambda or a local func",
            ):
                list(Stream(src).map(f, concurrency=2, via="process"))

        sleeps = [0.01, 1, 0.01]
        state: List[str] = []
        expected_result_list: List[str] = list(order_mutation(map(str, sleeps)))
        stream = (
            Stream(sleeps)
            .foreach(identity_sleep, concurrency=2, ordered=ordered, via="process")
            .map(str, concurrency=2, ordered=True, via="process")
            .foreach(state.append, concurrency=2, ordered=True, via="process")
            .foreach(lambda _: state.append(""), concurrency=1, ordered=True)
        )
        self.assertListEqual(
            list(stream),
            expected_result_list,
            msg="process-based concurrency must correctly transform elements, respecting `ordered`...",
        )
        self.assertListEqual(
            state,
            [""] * len(sleeps),
            msg="... and must not mutate main thread-bound structures.",
        )
        # test partial iteration:
        self.assertEqual(
            next(iter(stream)),
            expected_result_list[0],
            msg="process-based concurrency must behave ok with partial iteration",
        )

    @parameterized.expand(
        [
            [16, 0],
            [1, 0],
            [16, 1],
            [16, 15],
            [16, 16],
        ]
    )
    def test_map_with_more_concurrency_than_elements(
        self, concurrency, n_elems
    ) -> None:
        self.assertListEqual(
            list(Stream(range(n_elems)).map(str, concurrency=concurrency)),
            list(map(str, range(n_elems))),
            msg="`map` method should act correctly when concurrency > number of elements.",
        )

    @parameterized.expand(
        [
            [
                ordered,
                order_mutation,
                expected_duration,
                operation,
                func,
            ]
            for ordered, order_mutation, expected_duration in [
                (True, identity, 0.3),
                (False, sorted, 0.21),
            ]
            for operation, func in [
                (Stream.foreach, time.sleep),
                (Stream.map, identity_sleep),
                (Stream.aforeach, asyncio.sleep),
                (Stream.amap, async_identity_sleep),
            ]
        ]
    )
    def test_mapping_ordering(
        self,
        ordered: bool,
        order_mutation: Callable[[Iterable[float]], Iterable[float]],
        expected_duration: float,
        operation,
        func,
    ) -> None:
        seconds = [0.1, 0.01, 0.2]
        duration, res = timestream(
            operation(Stream(seconds), func, ordered=ordered, concurrency=2),
            5,
        )
        self.assertListEqual(
            res,
            list(order_mutation(seconds)),
            msg=f"`{operation}` must respect `ordered` constraint.",
        )

        self.assertAlmostEqual(
            duration,
            expected_duration,
            msg=f"{'ordered' if ordered else 'unordered'} `{operation}` should reflect that unordering improves runtime by avoiding bottlenecks",
            delta=expected_duration * 0.2,
        )

    @parameterized.expand(
        [
            [1],
            [2],
        ]
    )
    def test_foreach(self, concurrency) -> None:
        side_collection: Set[int] = set()

        def side_effect(x: int, func: Callable[[int], int]):
            nonlocal side_collection
            side_collection.add(func(x))

        res = list(
            Stream(src).foreach(
                lambda i: randomly_slowed(side_effect(i, square)),
                concurrency=concurrency,
            )
        )

        self.assertListEqual(
            res,
            list(src),
            msg="At any concurrency the `foreach` method should return the upstream elements in order.",
        )
        self.assertSetEqual(
            side_collection,
            set(map(square, src)),
            msg="At any concurrency the `foreach` method should call func on upstream elements (in any order).",
        )

    @parameterized.expand(
        [
            [
                raised_exc,
                catched_exc,
                concurrency,
                method,
                throw_func_,
                throw_for_odd_func_,
            ]
            for raised_exc, catched_exc in [
                (TestError, TestError),
                (StopIteration, (NoopStopIteration, RuntimeError)),
            ]
            for concurrency in [1, 2]
            for method, throw_func_, throw_for_odd_func_ in [
                (Stream.foreach, throw_func, throw_for_odd_func),
                (Stream.map, throw_func, throw_for_odd_func),
                (Stream.amap, async_throw_func, async_throw_for_odd_func),
            ]
        ]
    )
    def test_map_or_foreach_with_exception(
        self,
        raised_exc: Type[Exception],
        catched_exc: Type[Exception],
        concurrency: int,
        method: Callable[[Stream, Callable[[Any], int], int], Stream],
        throw_func: Callable[[Exception], Callable[[Any], int]],
        throw_for_odd_func: Callable[[Exception], Callable[[Any], int]],
    ) -> None:
        with self.assertRaises(
            catched_exc,
            msg="At any concurrency, `map` and `foreach` and `amap` must raise",
        ):
            list(method(Stream(src), throw_func(raised_exc), concurrency))  # type: ignore

        self.assertListEqual(
            list(
                method(Stream(src), throw_for_odd_func(raised_exc), concurrency).catch(catched_exc)  # type: ignore
            ),
            list(pair_src),
            msg="At any concurrency, `map` and `foreach` and `amap` must not stop after one exception occured.",
        )

    @parameterized.expand(
        [
            [method, func, concurrency]
            for method, func in [
                (Stream.foreach, slow_identity),
                (Stream.map, slow_identity),
                (Stream.amap, async_slow_identity),
            ]
            for concurrency in [1, 2, 4]
        ]
    )
    def test_map_and_foreach_concurrency(self, method, func, concurrency) -> None:
        expected_iteration_duration = N * slow_identity_duration / concurrency
        duration, res = timestream(method(Stream(src), func, concurrency=concurrency))
        self.assertListEqual(res, list(src))
        self.assertAlmostEqual(
            duration,
            expected_iteration_duration,
            delta=expected_iteration_duration * DELTA_RATE,
            msg="Increasing the concurrency of mapping should decrease proportionnally the iteration's duration.",
        )

    @parameterized.expand(
        [
            [1],
            [2],
        ]
    )
    def test_flatten(self, concurrency) -> None:
        n_iterables = 32
        it = list(range(N // n_iterables))
        double_it = it + it
        iterables_stream = Stream(
            lambda: map(slow_identity, [double_it] + [it for _ in range(n_iterables)])
        )
        self.assertCountEqual(
            list(iterables_stream.flatten(concurrency=concurrency)),
            list(it) * n_iterables + double_it,
            msg="At any concurrency the `flatten` method should yield all the upstream iterables' elements.",
        )
        self.assertListEqual(
            list(
                Stream([iter([]) for _ in range(2000)]).flatten(concurrency=concurrency)
            ),
            [],
            msg="`flatten` should not yield any element if upstream elements are empty iterables, and be resilient to recursion issue in case of successive empty upstream iterables.",
        )

        with self.assertRaises(
            TypeError,
            msg="`flatten` should raise if an upstream element is not iterable.",
        ):
            next(iter(Stream(cast(Iterable, src)).flatten()))

    def test_flatten_concurrency(self) -> None:
        iterable_size = 5
        runtime, res = timestream(
            Stream(
                lambda: [
                    Stream(map(slow_identity, ["a"] * iterable_size)),
                    Stream(map(slow_identity, ["b"] * iterable_size)),
                    Stream(map(slow_identity, ["c"] * iterable_size)),
                ]
            ).flatten(concurrency=2),
            times=3,
        )
        self.assertEqual(
            res,
            ["a", "b"] * iterable_size + ["c"] * iterable_size,
            msg="`flatten` should process 'a's and 'b's concurrently and then 'c's",
        )
        a_runtime = b_runtime = c_runtime = iterable_size * slow_identity_duration
        expected_runtime = (a_runtime + b_runtime) / 2 + c_runtime
        self.assertAlmostEqual(
            runtime,
            expected_runtime,
            delta=DELTA_RATE * expected_runtime,
            msg="`flatten` should process 'a's and 'b's concurrently and then 'c's without concurrency",
        )

    def test_flatten_typing(self) -> None:
        flattened_iterator_stream: Stream[str] = Stream("abc").map(iter).flatten()
        flattened_list_stream: Stream[str] = Stream("abc").map(list).flatten()
        flattened_set_stream: Stream[str] = Stream("abc").map(set).flatten()
        flattened_map_stream: Stream[str] = (
            Stream("abc").map(lambda char: map(lambda x: x, char)).flatten()
        )
        flattened_filter_stream: Stream[str] = (
            Stream("abc").map(lambda char: filter(lambda _: True, char)).flatten()
        )

    @parameterized.expand(
        [
            [exception_type, mapped_exception_type, concurrency]
            for exception_type, mapped_exception_type in [
                (TestError, TestError),
                (StopIteration, NoopStopIteration),
            ]
            for concurrency in [1, 2]
        ]
    )
    def test_flatten_with_exception(
        self,
        exception_type: Type[Exception],
        mapped_exception_type: Type[Exception],
        concurrency: int,
    ) -> None:
        n_iterables = 5

        class IterableRaisingInIter(Iterable[int]):
            def __iter__(self) -> Iterator[int]:
                raise exception_type

        self.assertSetEqual(
            set(
                Stream(
                    map(
                        lambda i: (
                            IterableRaisingInIter() if i % 2 else range(i, i + 1)
                        ),
                        range(n_iterables),
                    )
                )
                .flatten(concurrency=concurrency)
                .catch(mapped_exception_type)
            ),
            set(range(0, n_iterables, 2)),
            msg="At any concurrency the `flatten` method should be resilient to exceptions thrown by iterators, especially it should remap StopIteration one to PacifiedStopIteration.",
        )

        class IteratorRaisingInNext(Iterator[int]):
            def __init__(self) -> None:
                self.first_next = True

            def __iter__(self) -> Iterator[int]:
                return self

            def __next__(self) -> int:
                if not self.first_next:
                    raise StopIteration
                self.first_next = False
                raise exception_type

        self.assertSetEqual(
            set(
                Stream(
                    map(
                        lambda i: (
                            IteratorRaisingInNext() if i % 2 else range(i, i + 1)
                        ),
                        range(n_iterables),
                    )
                )
                .flatten(concurrency=concurrency)
                .catch(mapped_exception_type)
            ),
            set(range(0, n_iterables, 2)),
            msg="At any concurrency the `flatten` method should be resilient to exceptions thrown by iterators, especially it should remap StopIteration one to PacifiedStopIteration.",
        )

    @parameterized.expand([[concurrency] for concurrency in [2, 4]])
    def test_partial_iteration_on_streams_using_concurrency(
        self, concurrency: int
    ) -> None:
        yielded_elems = []

        def remembering_src() -> Iterator[int]:
            nonlocal yielded_elems
            for elem in src:
                yielded_elems.append(elem)
                yield elem

        for stream, n_pulls_after_first_next in [
            (
                Stream(remembering_src).map(identity, concurrency=concurrency),
                concurrency + 1,
            ),
            (
                Stream(remembering_src).amap(async_identity, concurrency=concurrency),
                concurrency + 1,
            ),
            (
                Stream(remembering_src).foreach(identity, concurrency=concurrency),
                concurrency + 1,
            ),
            (
                Stream(remembering_src).aforeach(
                    async_identity, concurrency=concurrency
                ),
                concurrency + 1,
            ),
            (
                Stream(remembering_src).group(1).flatten(concurrency=concurrency),
                concurrency,
            ),
        ]:
            yielded_elems = []
            iterator = iter(stream)
            time.sleep(0.5)
            self.assertEqual(
                len(yielded_elems),
                0,
                msg=f"before the first call to `next` a concurrent {type(stream)} should have pulled 0 upstream elements.",
            )
            next(iterator)
            time.sleep(0.5)
            self.assertEqual(
                len(yielded_elems),
                n_pulls_after_first_next,
                msg=f"`after the first call to `next` a concurrent {type(stream)} with concurrency={concurrency} should have pulled only {n_pulls_after_first_next} upstream elements.",
            )

    def test_filter(self) -> None:
        def keep(x) -> Any:
            return x % 2

        self.assertListEqual(
            list(Stream(src).filter(keep)),
            list(filter(keep, src)),
            msg="`filter` must act like builtin filter",
        )
        self.assertListEqual(
            list(Stream(src).filter()),
            list(filter(None, src)),
            msg="`filter` without predicate must act like builtin filter with None predicate.",
        )

    def test_truncate(self) -> None:
        with self.assertRaisesRegex(
            ValueError,
            "`count` and `when` can't be both None.",
        ):
            Stream(src).truncate()

        self.assertEqual(
            list(Stream(src).truncate(N * 2)),
            list(src),
            msg="`truncate` must be ok with count >= stream length",
        )
        self.assertEqual(
            list(Stream(src).truncate(2)),
            [0, 1],
            msg="`truncate` must be ok with count >= 1",
        )
        self.assertEqual(
            list(Stream(src).truncate(1)),
            [0],
            msg="`truncate` must be ok with count == 1",
        )
        self.assertEqual(
            list(Stream(src).truncate(0)),
            [],
            msg="`truncate` must be ok with count == 0",
        )

        with self.assertRaises(
            ValueError,
            msg="`truncate` must raise ValueError if `count` is negative",
        ):
            Stream(src).truncate(-1)

        with self.assertRaises(
            ValueError,
            msg="`truncate` must raise ValueError if `count` is float('inf')",
        ):
            Stream(src).truncate(cast(int, float("inf")))

        count = N // 2
        raising_stream_iterator = iter(
            Stream(lambda: map(lambda x: round((1 / x) * x**2), src)).truncate(count)
        )

        with self.assertRaises(
            ZeroDivisionError,
            msg="`truncate` must not stop iteration when encountering exceptions and raise them without counting them...",
        ):
            next(raising_stream_iterator)

        self.assertEqual(list(raising_stream_iterator), list(range(1, count + 1)))

        with self.assertRaises(
            StopIteration,
            msg="... and after reaching the limit it still continues to raise StopIteration on calls to next",
        ):
            next(raising_stream_iterator)

        iter_truncated_on_predicate = iter(Stream(src).truncate(when=lambda n: n == 5))
        self.assertEqual(
            list(iter_truncated_on_predicate),
            list(Stream(src).truncate(5)),
            msg="`when` n == 5 must be equivalent to `count` = 5",
        )
        with self.assertRaises(
            StopIteration,
            msg="After exhaustion a call to __next__ on a truncated iterator must raise StopIteration",
        ):
            next(iter_truncated_on_predicate)

        with self.assertRaises(
            ZeroDivisionError,
            msg="an exception raised by `when` must be raised",
        ):
            list(Stream(src).truncate(when=lambda _: 1 / 0))

        self.assertEqual(
            list(Stream(src).truncate(6, when=lambda n: n == 5)),
            list(range(5)),
            msg="`when` and `count` argument can be set at the same time, and the truncation should happen as soon as one or the other is satisfied.",
        )

        self.assertEqual(
            list(Stream(src).truncate(5, when=lambda n: n == 6)),
            list(range(5)),
            msg="`when` and `count` argument can be set at the same time, and the truncation should happen as soon as one or the other is satisfied.",
        )

    def test_group(self) -> None:
        # behavior with invalid arguments
        for seconds in [-1, 0]:
            with self.assertRaises(
                ValueError,
                msg="`group` should raise error when called with `seconds` <= 0.",
            ):
                list(
                    Stream([1]).group(
                        size=100, interval=datetime.timedelta(seconds=seconds)
                    )
                ),
        for size in [-1, 0]:
            with self.assertRaises(
                ValueError,
                msg="`group` should raise error when called with `size` < 1.",
            ):
                list(Stream([1]).group(size=size)),

        # group size
        self.assertListEqual(
            list(Stream(range(6)).group(size=4)),
            [[0, 1, 2, 3], [4, 5]],
            msg="",
        )
        self.assertListEqual(
            list(Stream(range(6)).group(size=2)),
            [[0, 1], [2, 3], [4, 5]],
            msg="",
        )
        self.assertListEqual(
            list(Stream([]).group(size=2)),
            [],
            msg="",
        )

        # behavior with exceptions
        def f(i):
            return i / (110 - i)

        stream_iterator = iter(Stream(lambda: map(f, src)).group(100))
        next(stream_iterator)
        self.assertListEqual(
            next(stream_iterator),
            list(map(f, range(100, 110))),
            msg="when encountering upstream exception, `group` should yield the current accumulated group...",
        )

        with self.assertRaises(
            ZeroDivisionError,
            msg="... and raise the upstream exception during the next call to `next`...",
        ):
            next(stream_iterator)

        self.assertListEqual(
            next(stream_iterator),
            list(map(f, range(111, 211))),
            msg="... and restarting a fresh group to yield after that.",
        )

        # behavior of the `seconds` parameter
        self.assertListEqual(
            list(
                Stream(lambda: map(slow_identity, src)).group(
                    size=100,
                    interval=datetime.timedelta(seconds=slow_identity_duration / 1000),
                )
            ),
            list(map(lambda e: [e], src)),
            msg="`group` should not yield empty groups even though `interval` if smaller than upstream's frequency",
        )
        self.assertListEqual(
            list(
                Stream(lambda: map(slow_identity, src)).group(
                    size=100,
                    interval=datetime.timedelta(seconds=slow_identity_duration / 1000),
                    by=lambda _: None,
                )
            ),
            list(map(lambda e: [e], src)),
            msg="`group` with `by` argument should not yield empty groups even though `interval` if smaller than upstream's frequency",
        )
        self.assertListEqual(
            list(
                Stream(lambda: map(slow_identity, src)).group(
                    size=100,
                    interval=datetime.timedelta(
                        seconds=2 * slow_identity_duration * 0.99
                    ),
                )
            ),
            list(map(lambda e: [e, e + 1], pair_src)),
            msg="`group` should yield upstream elements in a two-element group if `interval` inferior to twice the upstream yield period",
        )

        self.assertListEqual(
            next(iter(Stream(src).group())),
            list(src),
            msg="`group` without arguments should group the elements all together",
        )

        # test by
        stream_iter = iter(Stream(src).group(size=2, by=lambda n: n % 2))
        self.assertListEqual(
            [next(stream_iter), next(stream_iter)],
            [[0, 2], [1, 3]],
            msg="`group` called with a `by` function must cogroup elements.",
        )

        self.assertListEqual(
            next(
                iter(
                    Stream(src_raising_at_exhaustion).group(
                        size=10, by=lambda n: n % 4 != 0
                    )
                )
            ),
            [1, 2, 3, 5, 6, 7, 9, 10, 11, 13],
            msg="`group` called with a `by` function and a `size` should yield the first batch becoming full.",
        )

        self.assertListEqual(
            list(Stream(src).group(by=lambda n: n % 2)),
            [list(range(0, N, 2)), list(range(1, N, 2))],
            msg="`group` called with a `by` function and an infinite size must cogroup elements and yield groups starting with the group containing the oldest element.",
        )

        self.assertListEqual(
            list(Stream(range(10)).group(by=lambda n: n % 4 == 0)),
            [[0, 4, 8], [1, 2, 3, 5, 6, 7, 9]],
            msg="`group` called with a `by` function and reaching exhaustion must cogroup elements and yield uncomplete groups starting with the group containing the oldest element, even though it's not the largest.",
        )

        stream_iter = iter(Stream(src_raising_at_exhaustion).group(by=lambda n: n % 2))
        self.assertListEqual(
            [next(stream_iter), next(stream_iter)],
            [list(range(0, N, 2)), list(range(1, N, 2))],
            msg="`group` called with a `by` function and encountering an exception must cogroup elements and yield uncomplete groups starting with the group containing the oldest element.",
        )
        with self.assertRaises(
            TestError,
            msg="`group` called with a `by` function and encountering an exception must raise it after all groups have been yielded",
        ):
            next(stream_iter)

        # test seconds + by
        self.assertListEqual(
            list(
                Stream(lambda: map(slow_identity, range(10))).group(
                    interval=datetime.timedelta(seconds=slow_identity_duration * 2.90),
                    by=lambda n: n % 4 == 0,
                )
            ),
            [[1, 2], [0, 4], [3, 5, 6, 7], [8], [9]],
            msg="`group` called with a `by` function must cogroup elements and yield the largest groups when `seconds` is reached event though it's not the oldest.",
        )

        stream_iter = iter(
            Stream(src).group(
                size=3, by=lambda n: throw(StopIteration) if n == 2 else n
            )
        )
        self.assertEqual(
            [next(stream_iter), next(stream_iter)],
            [[0], [1]],
            msg="`group` should yield incomplete groups when `by` raises",
        )
        with self.assertRaises(
            NoopStopIteration,
            msg="`group` should raise and skip `elem` if `by(elem)` raises",
        ):
            next(stream_iter)
        self.assertEqual(
            next(stream_iter),
            [3],
            msg="`group` should continue yielding after `by`'s exception has been raised.",
        )

    def test_throttle(self) -> None:
        # behavior with invalid arguments
        with self.assertRaises(
            ValueError,
            msg="`throttle` should raise error when called with `interval` is negative.",
        ):
            list(Stream([1]).throttle(interval=datetime.timedelta(microseconds=-1)))
        with self.assertRaises(
            ValueError,
            msg="`throttle` should raise error when called with `per_second` < 1.",
        ):
            list(Stream([1]).throttle(per_second=0))
        with self.assertRaises(
            ValueError,
            msg="`throttle` should raise error when called with `per_minute` < 1.",
        ):
            list(Stream([1]).throttle(per_minute=0))
        with self.assertRaises(
            ValueError,
            msg="`throttle` should raise error when called with `per_hour` < 1.",
        ):
            list(Stream([1]).throttle(per_hour=0))

        # test interval
        interval_seconds = 0.3
        super_slow_elem_pull_seconds = 1
        N = 10
        duration, res = timestream(
            Stream(
                map(
                    sidify(
                        lambda e: (
                            time.sleep(super_slow_elem_pull_seconds) if e == 0 else None
                        )
                    ),
                    range(N),
                )
            ).throttle(interval=datetime.timedelta(seconds=interval_seconds))
        )

        self.assertEqual(
            res,
            list(range(N)),
            msg="`throttle` with `interval` must yield upstream elements",
        )
        expected_duration = (N - 1) * interval_seconds + super_slow_elem_pull_seconds
        self.assertAlmostEqual(
            duration,
            expected_duration,
            delta=0.1 * expected_duration,
            msg="avoid bursts after very slow particular upstream elements",
        )

        self.assertEqual(
            next(
                iter(
                    Stream(src)
                    .throttle(interval=datetime.timedelta(seconds=0.2))
                    .throttle(interval=datetime.timedelta(seconds=0.1))
                )
            ),
            0,
            msg="`throttle` should avoid 'ValueError: sleep length must be non-negative' when upstream is slower than `interval`",
        )

        # test per_second

        N = 11
        assert N % 2
        duration, res = timestream(Stream(range(11)).throttle(per_second=2))
        self.assertEqual(
            res,
            list(range(11)),
            msg="`throttle` with `per_second` must yield upstream elements",
        )
        expected_duration = N // 2
        self.assertAlmostEqual(
            duration,
            expected_duration,
            delta=0.01 * expected_duration,
            msg="`throttle` must slow according to `per_second`",
        )

        # # per_second and per_minute
        # N = 11
        # assert N % 2
        # per_minute = 8
        # per_second = 2
        # duration, res = timestream(
        #     Stream(range(11)).throttle(per_second=per_second, per_minute=per_minute)
        # )
        # self.assertEqual(
        #     res,
        #     list(range(11)),
        #     msg="`throttle` with `per_second` must yield upstream elements",
        # )
        # expected_duration = (N // per_minute) * 60 + (N % per_minute) // per_second
        # self.assertAlmostEqual(
        #     duration,
        #     expected_duration,
        #     delta=0.01 * expected_duration,
        #     msg="`throttle` must slow according to `per_second` and `per_minute`",
        # )

        # test both

        expected_duration = 2
        for stream in [
            Stream(range(11)).throttle(
                per_second=5, interval=datetime.timedelta(seconds=0.01)
            ),
            Stream(range(10)).throttle(
                per_second=20, interval=datetime.timedelta(seconds=0.2)
            ),
        ]:
            duration, _ = timestream(stream)
            self.assertAlmostEqual(
                duration,
                expected_duration,
                delta=0.1 * expected_duration,
                msg="`throttle` with both `per_second` and `interval` set should follow the most restrictive",
            )

    def test_catch(self) -> None:
        self.assertEqual(
            list(Stream(src).catch(finally_raise=True)),
            list(src),
            msg="`catch` should yield elements in exception-less scenarios",
        )
        with self.assertRaisesRegex(
            TypeError,
            "`iterator` should be an Iterator, but got a <class 'list'>",
            msg="`catch` function should raise TypError when first argument is not an Iterator",
        ):
            from streamable.functions import catch

            catch(cast(Iterator[int], [3, 4]))

        def f(i):
            return i / (3 - i)

        stream = Stream(lambda: map(f, src))
        safe_src = list(src)
        del safe_src[3]
        self.assertListEqual(
            list(stream.catch(ZeroDivisionError)),
            list(map(f, safe_src)),
            msg="If the exception type matches the `kind`, then the impacted element should be ignored.",
        )
        self.assertListEqual(
            list(stream.catch()),
            list(map(f, safe_src)),
            msg="If the predicate is not provided, then all exceptions should be catched.",
        )

        with self.assertRaises(
            ZeroDivisionError,
            msg="If a non catched exception type occurs, then it should be raised.",
        ):
            list(stream.catch(TestError))

        first_value = 1
        second_value = 2
        third_value = 3
        functions = [
            lambda: throw(TestError),
            lambda: throw(TypeError),
            lambda: first_value,
            lambda: second_value,
            lambda: throw(ValueError),
            lambda: third_value,
            lambda: throw(ZeroDivisionError),
        ]

        erroring_stream: Stream[int] = Stream(lambda: map(lambda f: f(), functions))
        for catched_erroring_stream in [
            erroring_stream.catch(finally_raise=True),
            erroring_stream.catch(Exception, finally_raise=True),
        ]:
            erroring_stream_iterator = iter(catched_erroring_stream)
            self.assertEqual(
                next(erroring_stream_iterator),
                first_value,
                msg="`catch` should yield the first non exception throwing element.",
            )
            n_yields = 1
            with self.assertRaises(
                TestError,
                msg="`catch` should raise the first error encountered when `finally_raise` is True.",
            ):
                for _ in erroring_stream_iterator:
                    n_yields += 1
            with self.assertRaises(
                StopIteration,
                msg="`catch` with `finally_raise`=True should finally raise StopIteration to avoid infinite recursion if there is another catch downstream.",
            ):
                next(erroring_stream_iterator)
            self.assertEqual(
                n_yields,
                3,
                msg="3 elements should have passed been yielded between catched exceptions.",
            )

        only_catched_errors_stream = Stream(
            map(lambda _: throw(TestError), range(2000))
        ).catch(TestError)
        self.assertEqual(
            list(only_catched_errors_stream),
            [],
            msg="When upstream raise exceptions without yielding any element, listing the stream must return empty list, without recursion issue.",
        )
        with self.assertRaises(
            StopIteration,
            msg="When upstream raise exceptions without yielding any element, then the first call to `next` on a stream catching all errors should raise StopIteration.",
        ):
            next(iter(only_catched_errors_stream))

        iterator = iter(
            Stream(map(throw, [TestError, ValueError]))
            .catch(ValueError, finally_raise=True)
            .catch(TestError, finally_raise=True)
        )
        with self.assertRaises(
            ValueError,
            msg="With 2 chained `catch`s with `finally_raise=True`, the error catched by the first `catch` is finally raised first (even though it was raised second)...",
        ):
            next(iterator)
        with self.assertRaises(
            TestError,
            msg="... and then the error catched by the second `catch` is raised...",
        ):
            next(iterator)
        with self.assertRaises(
            StopIteration,
            msg="... and a StopIteration is raised next.",
        ):
            next(iterator)

        with self.assertRaises(
            TypeError,
            msg="`catch` does not catch if `when` not satisfied",
        ):
            list(
                Stream(map(throw, [ValueError, TypeError])).catch(
                    Exception, when=lambda exception: "ValueError" in repr(exception)
                )
            )

        self.assertEqual(
            list(
                Stream(map(lambda n: 1 / n, [0, 1, 2, 4])).catch(
                    ZeroDivisionError, replacement=float("inf")
                )
            ),
            [float("inf"), 1, 0.5, 0.25],
            msg="`catch` should be able to yield a non-None replacement",
        )
        self.assertEqual(
            list(
                Stream(map(lambda n: 1 / n, [0, 1, 2, 4])).catch(
                    ZeroDivisionError, replacement=cast(float, None)
                )
            ),
            [None, 1, 0.5, 0.25],
            msg="`catch` should be able to yield a None replacement",
        )

    def test_observe(self) -> None:
        value_error_rainsing_stream: Stream[List[int]] = (
            Stream("123--678")
            .throttle(10)
            .observe("chars")
            .map(int)
            .observe("ints")
            .group(2)
            .observe("int pairs")
        )

        self.assertListEqual(
            list(value_error_rainsing_stream.catch(ValueError)),
            [[1, 2], [3], [6, 7], [8]],
            msg="This can break due to `group`/`map`/`catch`, check other breaking tests to determine quickly if it's an issue with `observe`.",
        )

        with self.assertRaises(
            ValueError,
            msg="`observe` should forward-raise exceptions",
        ):
            list(value_error_rainsing_stream)

    def test_is_iterable(self) -> None:
        self.assertIsInstance(Stream(src), Iterable)

    def test_count(self) -> None:
        l: List[int] = []

        def effect(x: int) -> None:
            nonlocal l
            l.append(x)

        stream = Stream(lambda: map(effect, src))
        self.assertEqual(
            stream.count(),
            N,
            msg="`count` should return the count of elements.",
        )
        self.assertListEqual(
            l, list(src), msg="`count` should iterate over the entire stream."
        )

    def test_call(self) -> None:
        l: List[int] = []
        stream = Stream(src).map(l.append)
        self.assertIs(
            stream(),
            stream,
            msg="`__call__` should return the stream.",
        )
        self.assertEqual(
            l,
            list(src),
            msg="`__call__` should exhaust the stream.",
        )

    def test_multiple_iterations(self) -> None:
        stream = Stream(src)
        for _ in range(3):
            self.assertEqual(
                list(stream),
                list(src),
                msg="The first iteration over a stream should yield the same elements as any subsequent iteration on the same stream, even if it is based on a `source` returning an iterator that only support 1 iteration.",
            )

    @parameterized.expand(
        [
            [1],
            [100],
        ]
    )
    def test_amap(self, concurrency) -> None:
        self.assertListEqual(
            list(
                Stream(src).amap(
                    async_randomly_slowed(async_square), concurrency=concurrency
                )
            ),
            list(map(square, src)),
            msg="At any concurrency the `amap` method should act as the builtin map function, transforming elements while preserving input elements order.",
        )
        stream = Stream(src).amap(identity)  # type: ignore
        with self.assertRaisesRegex(
            TypeError,
            "The function is expected to be an async function, i.e. it must be a function returning a Coroutine object, but returned a <class 'int'>.",
            msg="`amap` should raise a TypeError if a non async function is passed to it.",
        ):
            next(iter(stream))

    @parameterized.expand(
        [
            [1],
            [100],
        ]
    )
    def test_aforeach(self, concurrency) -> None:
        self.assertListEqual(
            list(
                Stream(src).aforeach(
                    async_randomly_slowed(async_square), concurrency=concurrency
                )
            ),
            list(src),
            msg="At any concurrency the `foreach` method must preserve input elements order.",
        )
        stream = Stream(src).aforeach(identity)  # type: ignore
        with self.assertRaisesRegex(
            TypeError,
            "The function is expected to be an async function, i.e. it must be a function returning a Coroutine object, but returned a <class 'int'>.",
            msg="`aforeach` should raise a TypeError if a non async function is passed to it.",
        ):
            next(iter(stream))

    def test_eq(self) -> None:
        stream = Stream(src)
        self.assertEqual(
            stream,
            stream,
        )
        same_stream = Stream(src)
        self.assertEqual(
            stream,
            same_stream,
        )
        self.assertEqual(
            stream.foreach(identity),
            same_stream.foreach(identity),
        )
        self.assertEqual(
            stream.foreach(identity).truncate(10),
            same_stream.foreach(identity).truncate(10),
        )
        self.assertNotEqual(
            stream.foreach(print).truncate(10),
            same_stream.foreach(identity).truncate(10),
        )
