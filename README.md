# ༄ `streamable`

***Pythonic Stream-like manipulation of iterables***

[![codecov](https://codecov.io/gh/ebonnal/streamable/graph/badge.svg?token=S62T0JQK9N)](https://codecov.io/gh/ebonnal/streamable)
[![unittest](https://github.com/ebonnal/streamable/actions/workflows/unittest.yml/badge.svg?branch=main)](https://github.com/ebonnal/streamable/actions)
[![typing](https://github.com/ebonnal/streamable/actions/workflows/typing.yml/badge.svg?branch=main)](https://github.com/ebonnal/streamable/actions)
[![lint](https://github.com/ebonnal/streamable/actions/workflows/lint.yml/badge.svg?branch=main)](https://github.com/ebonnal/streamable/actions)
[![PyPI](https://github.com/ebonnal/streamable/actions/workflows/pypi.yml/badge.svg?branch=main)](https://pypi.org/project/streamable)


||
|--|
|🔗 ***Fluent*** chainable operations|
|💤 ***Lazy*** operations|
|🔀 ***Concurrent*** via *threads*/*processes*/`asyncio`|
|🇹 ***Typed***, fully annotated, `Stream[T]` is an `Iterable[T]`|
|🛡️ ***Tested*** extensively with **Python 3.7 to 3.14**|
|🪶 ***Light***, no dependencies|


## 1. install

```bash
pip install streamable
```

## 2. import
```python
from streamable import Stream
```

## 3. init

Create a `Stream[T]` *decorating* an `Iterable[T]`:

```python
integers: Stream[int] = Stream(range(10))
```

## 4. operate

Chain ***lazy*** operations (only evaluated during iteration), each returning a new ***immutable*** `Stream`:

```python
inverses: Stream[float] = (
    integers
    .map(lambda n: round(1 / n, 2))
    .catch(ZeroDivisionError)
)
```

## 5. iterate

Iterate over a `Stream[T]` just as you would over any other `Iterable[T]`, elements are processed *on-the-fly*:

- **collect**
```python
>>> list(inverses)
[1.0, 0.5, 0.33, 0.25, 0.2, 0.17, 0.14, 0.12, 0.11]
>>> set(inverses)
{0.5, 1.0, 0.2, 0.33, 0.25, 0.17, 0.14, 0.12, 0.11}
```

- **reduce**
```python
>>> sum(inverses)
2.82
>>> from functools import reduce
>>> reduce(..., inverses)
```

- **loop**
```python
>>> for inverse in inverses:
>>>    ...
```

- **next**
```python
>>> next(iter(inverses))
1.0
```

# 📒 ***Operations***

*A dozen expressive lazy operations and that’s it!*

## `.map`

> Applies a transformation on elements:

```python
negative_integer_strings: Stream[str] = (
    integers
    .map(lambda n: -n)
    .map(str)
)

assert list(negative_integer_strings) == ['0', '-1', '-2', '-3', '-4', '-5', '-6', '-7', '-8', '-9']
```

### thread-based concurrency

> Applies the transformation via `concurrency` threads:

```python
import requests

pokemon_names: Stream[str] = (
    Stream(range(1, 4))
    .map(lambda i: f"https://pokeapi.co/api/v2/pokemon-species/{i}")
    .map(requests.get, concurrency=3)
    .map(requests.Response.json)
    .map(lambda poke: poke["name"])
)
assert list(pokemon_names) == ['bulbasaur', 'ivysaur', 'venusaur']
```

> Preserves the upstream order by default (FIFO), but you can set `ordered=False` for ***First Done First Out***.

> [!NOTE]
> `concurrency` is also the size of the buffer containing not-yet-yielded results. **If the buffer is full, the iteration over the upstream is paused** until a result is yielded from the buffer.

> [!TIP]
> The performance of thread-based concurrency in a CPU-bound script can be drastically improved by using a [Python 3.13+ free-threaded build](https://docs.python.org/3/using/configure.html#cmdoption-disable-gil).

### process-based concurrency

> Set `via="process"`:

```python
if __name__ == "__main__":
    state: List[int] = []
    # integers are mapped
    assert integers.map(state.append, concurrency=4, via="process").count() == 10
    # but the `state` of the main process is not mutated
    assert state == []
```

### async-based concurrency

> The sibling operation `.amap` applies an async function:

```python
import httpx
import asyncio

http_async_client = httpx.AsyncClient()

pokemon_names: Stream[str] = (
    Stream(range(1, 4))
    .map(lambda i: f"https://pokeapi.co/api/v2/pokemon-species/{i}")
    .amap(http_async_client.get, concurrency=3)
    .map(httpx.Response.json)
    .map(lambda poke: poke["name"])
)

assert list(pokemon_names) == ['bulbasaur', 'ivysaur', 'venusaur']
asyncio.get_event_loop().run_until_complete(http_async_client.aclose())
```

### starmap

> The `star` function decorator transforms a function that takes several positional arguments into a function that takes a tuple:

```python
from streamable import star

zeros: Stream[int] = (
    Stream(enumerate(integers))
    .map(star(lambda index, integer: index - integer))
)

assert list(zeros) == [0, 0, 0, 0, 0, 0, 0, 0, 0, 0]
```

### "running map"

> {TODO: add description}

```python
from streamable import running

cumulative_sum: Stream[int] = (
    integers
    .map(running(lambda cumsum, i: cumsum + i, initial=0))
)

assert list(cumulative_sum) == [0, 1, 3, 6, 10, 15, 21, 28, 36, 45]
```



## `.foreach`



> Applies a side effect on elements:

```python
state: List[int] = []
appending_integers: Stream[int] = integers.foreach(state.append)

assert list(appending_integers) == [0, 1, 2, 3, 4, 5, 6, 7, 8, 9]
assert state == [0, 1, 2, 3, 4, 5, 6, 7, 8, 9]
```

### thread-based concurrency

> Like `.map` it has an optional `concurrency` parameter.
> Preserves the upstream order by default (FIFO), but you can set `ordered=False` for ***First Done First Out***.

### process-based concurrency

> Like for `.map`, set the parameter `via="process"`.

### async-based concurrency

> Like `.map` it has a sibling `.aforeach` operation for async.


## `.filter`



> Keeps only the elements that satisfy a condition:

```python
even_integers: Stream[int] = integers.filter(lambda n: n % 2 == 0)

assert list(even_integers) == [0, 2, 4, 6, 8]
```


## `.throttle`



> Limits the number of yields `per_second`/`per_minute`/`per_hour`:

```python
integers_5_per_sec: Stream[int] = integers.throttle(per_second=3)

# takes 3s: ceil(10 integers / 3 per_second) - 1
assert list(integers_5_per_sec) == [0, 1, 2, 3, 4, 5, 6, 7, 8, 9]
```

> and/or ensures a minimum `interval` between two successive yields:

```python
from datetime import timedelta

integers_every_100_millis = (
    integers
    .throttle(interval=timedelta(milliseconds=100))
)

# takes 900 millis: (10 integers - 1) * 100 millis
assert list(integers_every_100_millis) == [0, 1, 2, 3, 4, 5, 6, 7, 8, 9]
```


## `.group`



> Groups elements into `List`s:

```python
integers_by_5: Stream[List[int]] = integers.group(size=5)

assert list(integers_by_5) == [[0, 1, 2, 3, 4], [5, 6, 7, 8, 9]]
```
```python
integers_by_parity: Stream[List[int]] = integers.group(by=lambda n: n % 2)

assert list(integers_by_parity) == [[0, 2, 4, 6, 8], [1, 3, 5, 7, 9]]
```
```python
from datetime import timedelta

integers_within_1_sec: Stream[List[int]] = (
    integers
    .throttle(per_second=2)
    .group(interval=timedelta(seconds=0.99))
)

assert list(integers_within_1_sec) == [[0, 1, 2], [3, 4], [5, 6], [7, 8], [9]]
```

> Mix the `size`/`by`/`interval` parameters:
```python
integers_by_parity_by_2: Stream[List[int]] = (
    integers
    .group(by=lambda n: n % 2, size=2)
)

assert list(integers_by_parity_by_2) == [[0, 2], [1, 3], [4, 6], [5, 7], [8], [9]]
```


### `.groupby`



> Like `.group`, but groups into `(key, elements)` tuples:
```python
integers_by_parity: Stream[Tuple[str, List[int]]] = (
    integers
    .groupby(lambda n: "odd" if n % 2 else "even")
)

assert list(integers_by_parity) == [("even", [0, 2, 4, 6, 8]), ("odd", [1, 3, 5, 7, 9])]
```

> [!TIP]
> Then *"star map"* over the tuples:

```python
from streamable import star

counts_by_parity: Stream[Tuple[str, int]] = (
    integers_by_parity
    .map(star(lambda parity, ints: (parity, len(ints))))
)

assert list(counts_by_parity) == [("even", 5), ("odd", 5)]
```


## `.flatten`



> Ungroups elements assuming that they are `Iterable`s:

```python
even_then_odd_integers: Stream[int] = integers_by_parity.flatten()

assert list(even_then_odd_integers) == [0, 2, 4, 6, 8, 1, 3, 5, 7, 9]
```

### thread-based concurrency

> Flattens `concurrency` iterables concurrently:

```python
mixed_ones_and_zeros: Stream[int] = (
    Stream([[0] * 4, [1] * 4])
    .flatten(concurrency=2)
)
assert list(mixed_ones_and_zeros) == [0, 1, 0, 1, 0, 1, 0, 1]
```



## `.catch`



> Catches a given type of exceptions, and optionally yields a `replacement` value:

```python
inverses: Stream[float] = (
    integers
    .map(lambda n: round(1 / n, 2))
    .catch(ZeroDivisionError, replacement=float("inf"))
)

assert list(inverses) == [float("inf"), 1.0, 0.5, 0.33, 0.25, 0.2, 0.17, 0.14, 0.12, 0.11]
```

> You can specify an additional `when` condition for the catch:
```python
import requests
from requests.exceptions import ConnectionError

status_codes_ignoring_resolution_errors: Stream[int] = (
    Stream(["https://github.com", "https://foo.bar", "https://github.com/foo/bar"])
    .map(requests.get, concurrency=2)
    .catch(ConnectionError, when=lambda exception: "Max retries exceeded with url" in str(exception))
    .map(lambda response: response.status_code)
)

assert list(status_codes_ignoring_resolution_errors) == [200, 404]
```

> It has an optional `finally_raise: bool` parameter to raise the first catched exception when iteration ends.


## `.truncate`



> Ends iteration once a given number of elements have been yielded:

```python
five_first_integers: Stream[int] = integers.truncate(5)

assert list(five_first_integers) == [0, 1, 2, 3, 4]
```

> ... or when a condition has become satisfied:

```python
five_first_integers: Stream[int] = integers.truncate(when=lambda n: n == 5)

assert list(five_first_integers) == [0, 1, 2, 3, 4]
```


## `.skip`



> Skips the first specified number of elements:

```python
integers_after_five: Stream[int] = integers.skip(5)

assert list(integers_after_five) == [5, 6, 7, 8, 9]
```


## `.distinct`



> Removes duplicates:

```python
distinct_chars: Stream[str] = Stream("foobarfooo").distinct()

assert list(distinct_chars) == ["f", "o", "b", "a", "r"]
```

> Specify a function to deduplicate based on the value it returns when applied to elements:

```python
strings_of_distinct_lengths: Stream[str] = (
    Stream(["a", "foo", "bar", "z"])
    .distinct(len)
)

assert list(strings_of_distinct_lengths) == ["a", "foo"]
```

> [!WARNING]
> During iteration, all distinct elements that are yielded are retained in memory to perform deduplication. However, you can remove only consecutive duplicates without a memory footprint by setting `consecutive_only=True`:

```python
consecutively_distinct_chars: Stream[str] = (
    Stream("foobarfooo")
    .distinct(consecutive_only=True)
)

assert list(consecutively_distinct_chars) == ["f", "o", "b", "a", "r", "f", "o"]
```


## `.observe`



> Logs the progress of iterations:
```python
>>> assert list(integers.throttle(per_second=2).observe("integers")) == [0, 1, 2, 3, 4, 5, 6, 7, 8, 9]
```
```
INFO: [duration=0:00:00.001793 errors=0] 1 integers yielded
INFO: [duration=0:00:00.004388 errors=0] 2 integers yielded
INFO: [duration=0:00:01.003655 errors=0] 4 integers yielded
INFO: [duration=0:00:03.003196 errors=0] 8 integers yielded
INFO: [duration=0:00:04.003852 errors=0] 10 integers yielded
```

> [!NOTE]
> The amount of logs will never be overwhelming because they are produced logarithmically (base 2): the 11th log will be produced after 1,024 elements have been yielded, the 21th log after 1,048,576 elements, ...


## `+`



> Concatenates streams:

```python
assert list(integers + integers) == [0, 1, 2, 3 ,4, 5, 6, 7, 8, 9, 0, 1, 2, 3 ,4, 5, 6, 7, 8, 9]
```


## `zip`



> [!TIP]
> Use the standard `zip` function:

```python
from streamable import star

cubes: Stream[int] = (
    Stream(zip(integers, integers, integers))  # Stream[Tuple[int, int, int]]
    .map(star(lambda a, b, c: a * b * c))  # Stream[int]
)

assert list(cubes) == [0, 1, 8, 27, 64, 125, 216, 343, 512, 729]
```


## Shorthands for consuming the stream
> [!NOTE]
> Although consuming the stream is beyond the scope of this library, it provides two basic shorthands to trigger an iteration:

### `.count`



> Iterates over the stream until exhaustion and returns the number of elements yielded:

```python
assert integers.count() == 10
```


### `()`



> *Calling* the stream iterates over it until exhaustion and returns it:
```python
state: List[int] = []
appending_integers: Stream[int] = integers.foreach(state.append)
assert appending_integers() is appending_integers
assert state == [0, 1, 2, 3, 4, 5, 6, 7, 8, 9]
```



# 💡 Tips
## Extract-Transform-Load
> [!TIP]
> **Custom ETL scripts** can benefit from the expressiveness of this library. Below is a pipeline that extracts the 67 quadruped Pokémon from the first three generations using [PokéAPI](https://pokeapi.co/) and loads them into a CSV:

```python
import csv
from datetime import timedelta
import itertools
import requests
from streamable import Stream

with open("./quadruped_pokemons.csv", mode="w") as file:
    fields = ["id", "name", "is_legendary", "base_happiness", "capture_rate"]
    writer = csv.DictWriter(file, fields, extrasaction='ignore')
    writer.writeheader()

    pipeline: Stream = (
        # Infinite Stream[int] of Pokemon ids starting from Pokémon #1: Bulbasaur
        Stream(itertools.count(1))
        # Limits to 16 requests per second to be friendly to our fellow PokéAPI devs
        .throttle(per_second=16)
        # GETs pokemons concurrently using a pool of 8 threads
        .map(lambda poke_id: f"https://pokeapi.co/api/v2/pokemon-species/{poke_id}")
        .map(requests.get, concurrency=8)
        .foreach(requests.Response.raise_for_status)
        .map(requests.Response.json)
        # Stops the iteration when reaching the 1st pokemon of the 4th generation
        .truncate(when=lambda poke: poke["generation"]["name"] == "generation-iv")
        .observe("pokemons")
        # Keeps only quadruped Pokemons
        .filter(lambda poke: poke["shape"]["name"] == "quadruped")
        .observe("quadruped pokemons")
        # Catches errors due to None "generation" or "shape"
        .catch(
            TypeError,
            when=lambda error: str(error) == "'NoneType' object is not subscriptable"
        )
        # Writes a batch of pokemons every 5 seconds to the CSV file
        .group(interval=timedelta(seconds=5))
        .foreach(writer.writerows)
        .flatten()
        .observe("written pokemons")
        # Catches exceptions and raises the 1st one at the end of the iteration
        .catch(finally_raise=True)
    )

    pipeline()
```

## Visitor Pattern
> [!TIP]
> A `Stream` can be visited via its `.accept` method: implement a custom [***visitor***](https://en.wikipedia.org/wiki/Visitor_pattern) by extending the abstract class `streamable.visitors.Visitor`:

```python
from streamable.visitors import Visitor

class DepthVisitor(Visitor[int]):
    def visit_stream(self, stream: Stream) -> int:
        if not stream.upstream:
            return 1
        return 1 + stream.upstream.accept(self)

def depth(stream: Stream) -> int:
    return stream.accept(DepthVisitor())

assert depth(Stream(range(10)).map(str).filter()) == 3
```

## Functions
> [!TIP]
> The `Stream`'s methods are also exposed as functions:
```python
from streamable.functions import catch

inverse_integers: Iterator[int] = map(lambda n: 1 / n, range(10))
safe_inverse_integers: Iterator[int] = catch(inverse_integers, ZeroDivisionError)
```

## Logging Level
> [!TIP]
> This mutes the `.observe` operations which log at `INFO` level:

```python
import logging
logging.getLogger("streamable").setLevel(logging.WARNING)
```

# Contributing

Feel very welcome to:
- [open issues](https://github.com/ebonnal/streamable/issues)
- [open pull requests](https://github.com/ebonnal/streamable/pulls)
- check [CONTRIBUTING.md](CONTRIBUTING.md)

# 🙏 Thanks for the highlight to 
- [Tryolabs' Top Python libraries of 2024](https://tryolabs.com/blog/top-python-libraries-2024#top-10---general-use) ([LinkedIn](https://www.linkedin.com/posts/tryolabs_top-python-libraries-2024-activity-7273052840984539137-bcGs?utm_source=share&utm_medium=member_desktop), [Reddit](https://www.reddit.com/r/Python/comments/1hbs4t8/the_handpicked_selection_of_the_best_python/))
- [PyCoder’s Weekly](https://pycoders.com/issues/651) x [Real Python](https://realpython.com/)
- [@PythonHub's tweet](https://x.com/PythonHub/status/1842886311369142713)
- [Upvoters on our showcase Reddit post](https://www.reddit.com/r/Python/comments/1fp38jd/streamable_streamlike_manipulation_of_iterables/)
