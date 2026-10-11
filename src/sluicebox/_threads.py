"""State of its own for every writing thread, on free-threaded Python.

On free-threaded Python, a thread that reads an object it did not create updates the object's
reference count atomically. Threads that all read the same objects on every write - a lock, a
list, a cached string, an enum member, a large int - contend for those objects' cache lines,
more than for any lock, and several threads writing through one client end up slower than one.
So writes run on state each thread owns: the engine gives every writing thread a lane of its
own, the serializer a view with caches of its own, and the values read on every write are
immortal (interned strings, small ints, ``None``, ``True``) or private copies.

With the GIL, one thread runs at a time and per-thread state buys nothing: all threads share
one lane and one view, as they always did.
"""

from __future__ import annotations

import sys

__all__ = ["PER_THREAD", "per_thread", "private"]

#: Whether writing threads get state of their own. None decides by the interpreter:
#: free-threaded Python with the GIL disabled. Tests set it to cover both ways on any build.
PER_THREAD: bool | None = None


def per_thread() -> bool:
    if PER_THREAD is not None:
        return PER_THREAD
    gil_enabled = getattr(sys, "_is_gil_enabled", None)  # Python 3.13+
    return gil_enabled is not None and not gil_enabled()


def private(value: int) -> int:
    """An int equal to ``value``, created (and so owned) by the calling thread.

    Small ints are immortal and come back as they are.
    """
    return value + 0  # a new object: CPython does not return ``value`` itself for ``+ 0``
