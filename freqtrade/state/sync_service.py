"""Coordinator that refreshes data sources and updates the StateStore."""

from __future__ import annotations

import logging
from dataclasses import dataclass
from datetime import datetime
from threading import RLock
from typing import Callable, Dict

from .dtos import SnapshotBundle
from .store import StateStore

logger = logging.getLogger(__name__)

SyncProducer = Callable[["SyncContext"], SnapshotBundle]


@dataclass
class SyncContext:
    """Metadata provided to producers during refresh."""

    triggered_at: datetime
    sequence: int


class DataSyncService:
    """Runs registered producers and applies their bundles to the store."""

    def __init__(self, store: StateStore) -> None:
        self._store = store
        self._lock = RLock()
        self._producers: Dict[str, SyncProducer] = {}
        self._sequence = 0

    def register(self, name: str, producer: SyncProducer) -> None:
        with self._lock:
            if name in self._producers:
                raise ValueError(f"Producer '{name}' already registered")
            self._producers[name] = producer

    def unregister(self, name: str) -> None:
        with self._lock:
            self._producers.pop(name, None)

    def refresh_once(self) -> None:
        with self._lock:
            context = SyncContext(triggered_at=datetime.utcnow(), sequence=self._sequence + 1)
            producers = dict(self._producers)
            self._sequence = context.sequence

        for name, producer in producers.items():
            try:
                bundle = producer(context)
            except Exception as exc:  # pylint: disable=broad-except
                logger.exception("Producer '%s' failed: %s", name, exc)
                continue
            has_update = any(
                attr is not None
                for attr in (bundle.wallets, bundle.positions, bundle.orders, bundle.trades, bundle.forecasts)
            )
            if has_update:
                self._store.apply(bundle)

    def clear(self) -> None:
        """Helper used in tests to drop stored snapshots."""
        empty_bundle = SnapshotBundle(
            wallets=(), positions=(), orders=(), trades=(), forecasts=()
        )
        self._store.apply(empty_bundle, publish=False)
