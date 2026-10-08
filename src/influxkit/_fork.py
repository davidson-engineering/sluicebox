"""Fork safety (POSIX).

A child process starts with only the thread that called ``fork()``. Locks that other threads
held at that moment would stay locked forever in the child, and buffers, threads and
connections belong to the parent. Modules register what the child must redo here, and the
hooks run in a fixed order whatever the import order:

1. ``locks``: replace module-level locks with fresh ones;
2. ``state``: reset objects (this may run user callbacks, which may need those locks).
"""

from __future__ import annotations

import os
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from collections.abc import Callable

__all__ = ["on_child"]

_locks: list[Callable[[], None]] = []
_state: list[Callable[[], None]] = []


def on_child(*, locks: Callable[[], None] | None = None, state: Callable[[], None] | None = None) -> None:
    """Register functions to run in a forked child (``locks`` first, then ``state``)."""
    if locks is not None:
        _locks.append(locks)
    if state is not None:
        _state.append(state)


def _after_fork_in_child() -> None:
    for reset in _locks:
        reset()
    for reset in _state:
        reset()


if hasattr(os, "register_at_fork"):
    os.register_at_fork(after_in_child=_after_fork_in_child)
