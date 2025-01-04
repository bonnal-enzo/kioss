from typing import Any, Callable, Coroutine, Generic, Tuple, Type, TypeVar, overload

T = TypeVar("T")
R = TypeVar("R")


class WrappedError(Exception):
    def __init__(self, error: Exception):
        super().__init__(repr(error))
        self.error = error


class _ErrorWrappingDecorator(Generic[T, R]):
    def __init__(self, func: Callable[[T], R], error_type: Type[Exception]) -> None:
        self.func = func
        self.error_type = error_type

    def __call__(self, arg: T) -> R:
        try:
            return self.func(arg)
        except self.error_type as e:
            raise WrappedError(e) from e


def wrap_error(func: Callable[[T], R], error_type: Type[Exception]) -> Callable[[T], R]:
    return _ErrorWrappingDecorator(func, error_type)


iter_wo_stopiteration = wrap_error(iter, StopIteration)


class _Sidify(Generic[T]):
    def __init__(self, func: Callable[[T], Any]) -> None:
        self.func = func

    def __call__(self, arg: T) -> T:
        self.func(arg)
        return arg


def sidify(func: Callable[[T], Any]) -> Callable[[T], T]:
    return _Sidify(func)


def async_sidify(
    func: Callable[[T], Coroutine]
) -> Callable[[T], Coroutine[Any, Any, T]]:
    async def wrap(arg: T) -> T:
        coroutine = func(arg)
        if not isinstance(coroutine, Coroutine):
            raise TypeError(
                f"`transformation` must be an async function i.e. a function returning a Coroutine but it returned a {type(coroutine)}"
            )
        await coroutine
        return arg

    return wrap


class _Star(Generic[R]):
    def __init__(self, func: Callable[..., R]) -> None:
        self.func = func

    def __call__(self, args: Tuple) -> R:
        return self.func(*args)


T1 = TypeVar("T1")
T2 = TypeVar("T2")
T3 = TypeVar("T3")
T4 = TypeVar("T4")
T5 = TypeVar("T5")
T6 = TypeVar("T6")
T7 = TypeVar("T7")
T8 = TypeVar("T8")
T9 = TypeVar("T9")

# fmt: off
@overload
def star(func: Callable[[T], R]) -> Callable[[Tuple], R]: ...
@overload
def star(func: Callable[[T1, T2], R]) -> Callable[[Tuple[T1, T2]], R]: ...
@overload
def star(func: Callable[[T1, T2, T3], R]) -> Callable[[Tuple[T1, T2, T3]], R]: ...
@overload
def star(func: Callable[[T1, T2, T3, T4], R]) -> Callable[[Tuple[T1, T2, T3, T4]], R]: ...
@overload
def star(func: Callable[[T1, T2, T3, T4, T5], R]) -> Callable[[Tuple[T1, T2, T3, T4, T5]], R]: ...
@overload
def star(func: Callable[[T1, T2, T3, T4, T5, T6], R]) -> Callable[[Tuple[T1, T2, T3, T4, T5, T6]], R]: ...
@overload
def star(func: Callable[[T1, T2, T3, T4, T5, T6, T7], R]) -> Callable[[Tuple[T1, T2, T3, T4, T5, T6, T7]], R]: ...
@overload
def star(func: Callable[[T1, T2, T3, T4, T5, T6, T7, T8], R]) -> Callable[[Tuple[T1, T2, T3, T4, T5, T6, T7, T8]], R]: ...
@overload
def star(func: Callable[[T1, T2, T3, T4, T5, T6, T7, T8, T9], R]) -> Callable[[Tuple[T1, T2, T3, T4, T5, T6, T7, T8, T9]], R]: ...
@overload
def star(func: Callable[..., R]) -> Callable[[Tuple], R]: ...
# fmt: on


def star(func: Callable[..., R]) -> Callable[[Tuple], R]:
    """
    Transforms a function that takes several positional arguments into a function that takes a tuple.

    ```
    @star
    def add(a: int, b: int) -> int:
        return a + b

    assert add((2, 5)) == 7

    assert star(lambda a, b: a + b)((2, 5)) == 7
    ```
    """
    return _Star(func)


def running(func: Callable[[R, T], R], initial: R) -> Callable[[T], R]:
    """
    TODO
    """
    acc = initial
    def _(elem: T) -> R:
        nonlocal acc
        acc = func(acc, elem)
        return acc
    return _

