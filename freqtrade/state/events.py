"""Event bus used to publish state changes to interested components."""

from __future__ import annotations

from collections import defaultdict
from threading import RLock
from typing import Callable, DefaultDict, Iterable

from .dtos import SnapshotBundle

StateListener = Callable[[SnapshotBundle], None]


class StateEvents:
    """Simple observer registry keyed by event name."""

    def __init__(self) -> None:
        self._lock = RLock()
        self._listeners: DefaultDict[str, set[StateListener]] = defaultdict(set)

    def subscribe(self, event: str, listener: StateListener) -> None:
        with self._lock:
            self._listeners[event].add(listener)

    def unsubscribe(self, event: str, listener: StateListener) -> None:
        with self._lock:
            if listener in self._listeners.get(event, set()):
                self._listeners[event].remove(listener)
            if not self._listeners[event]:
                self._listeners.pop(event, None)

    def publish(self, event: str, bundle: SnapshotBundle) -> None:
        listeners: Iterable[StateListener]
        with self._lock:
            listeners = tuple(self._listeners.get(event, ()))
        for callback in listeners:
            callback(bundle)

