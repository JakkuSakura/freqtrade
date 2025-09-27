"""Thread-safe in-memory store for state snapshots."""

from __future__ import annotations

from threading import RLock
from typing import Iterable, Mapping, Sequence

from .dtos import (
    ForecastSnapshot,
    OrderSnapshot,
    PositionSnapshot,
    SnapshotBundle,
    TradeSnapshot,
    WalletSnapshot,
)
from .events import StateEvents


class StateStore:
    """Central repository for live snapshots consumed by the bot."""

    def __init__(self, events: StateEvents | None = None) -> None:
        self._lock = RLock()
        self._events = events
        self._wallets: dict[str, WalletSnapshot] = {}
        self._positions: dict[str, PositionSnapshot] = {}
        self._orders: dict[str, OrderSnapshot] = {}
        self._trades: dict[str, TradeSnapshot] = {}
        self._forecasts: dict[str, ForecastSnapshot] = {}
        self._version = 0

    @property
    def version(self) -> int:
        with self._lock:
            return self._version

    def apply(self, bundle: SnapshotBundle, publish: bool = True) -> None:
        with self._lock:
            if bundle.wallets is not None:
                self._wallets = {snap.currency: snap for snap in bundle.wallets}
            if bundle.positions is not None:
                self._positions = {snap.symbol: snap for snap in bundle.positions}
            if bundle.orders is not None:
                self._orders = {snap.order_id: snap for snap in bundle.orders}
            if bundle.trades is not None:
                self._trades = {snap.trade_id: snap for snap in bundle.trades}
            if bundle.forecasts is not None:
                self._forecasts = {snap.key: snap for snap in bundle.forecasts}
            self._version += 1
            current_version = self._version

        if publish and self._events:
            self._events.publish("state.updated", bundle)
            # Emit more granular events when present.
            if bundle.wallets is not None:
                self._events.publish("state.wallets", bundle)
            if bundle.positions is not None:
                self._events.publish("state.positions", bundle)
            if bundle.orders is not None or bundle.trades is not None:
                self._events.publish("state.oms", bundle)
            if bundle.forecasts is not None:
                self._events.publish("state.forecasts", bundle)

    def get_wallets(self) -> Mapping[str, WalletSnapshot]:
        with self._lock:
            return dict(self._wallets)

    def get_positions(self) -> Mapping[str, PositionSnapshot]:
        with self._lock:
            return dict(self._positions)

    def get_orders(self) -> Mapping[str, OrderSnapshot]:
        with self._lock:
            return dict(self._orders)

    def get_trades(self) -> Mapping[str, TradeSnapshot]:
        with self._lock:
            return dict(self._trades)

    def get_forecasts(self) -> Mapping[str, ForecastSnapshot]:
        with self._lock:
            return dict(self._forecasts)

    def iter_wallets(self) -> Iterable[WalletSnapshot]:
        with self._lock:
            return tuple(self._wallets.values())

    def iter_positions(self) -> Iterable[PositionSnapshot]:
        with self._lock:
            return tuple(self._positions.values())

    def iter_orders(self) -> Iterable[OrderSnapshot]:
        with self._lock:
            return tuple(self._orders.values())

    def iter_trades(self) -> Iterable[TradeSnapshot]:
        with self._lock:
            return tuple(self._trades.values())

    def iter_forecasts(self) -> Iterable[ForecastSnapshot]:
        with self._lock:
            return tuple(self._forecasts.values())

    def snapshot(self) -> SnapshotBundle:
        with self._lock:
            return SnapshotBundle(
                wallets=tuple(self._wallets.values()),
                positions=tuple(self._positions.values()),
                orders=tuple(self._orders.values()),
                trades=tuple(self._trades.values()),
                forecasts=tuple(self._forecasts.values()),
            )
