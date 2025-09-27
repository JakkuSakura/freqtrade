"""Public read-only facade around the StateStore."""

from __future__ import annotations

from typing import Iterable, Mapping

from .dtos import ForecastSnapshot, OrderSnapshot, PositionSnapshot, TradeSnapshot, WalletSnapshot
from .store import StateStore


class StateReader:
    """Provides convenient accessors for consumers."""

    def __init__(self, store: StateStore) -> None:
        self._store = store

    def wallets(self) -> Mapping[str, WalletSnapshot]:
        return self._store.get_wallets()

    def wallet(self, currency: str) -> WalletSnapshot | None:
        return self._store.get_wallets().get(currency)

    def positions(self) -> Mapping[str, PositionSnapshot]:
        return self._store.get_positions()

    def position(self, symbol: str) -> PositionSnapshot | None:
        return self._store.get_positions().get(symbol)

    def orders(self) -> Mapping[str, OrderSnapshot]:
        return self._store.get_orders()

    def order(self, order_id: str) -> OrderSnapshot | None:
        return self._store.get_orders().get(order_id)

    def trades(self) -> Mapping[str, TradeSnapshot]:
        return self._store.get_trades()

    def trade(self, trade_id: str) -> TradeSnapshot | None:
        return self._store.get_trades().get(trade_id)

    def forecasts(self) -> Mapping[str, ForecastSnapshot]:
        return self._store.get_forecasts()

    def forecast(self, key: str) -> ForecastSnapshot | None:
        return self._store.get_forecasts().get(key)

    def iter_wallets(self) -> Iterable[WalletSnapshot]:
        return self._store.iter_wallets()

    def iter_positions(self) -> Iterable[PositionSnapshot]:
        return self._store.iter_positions()

    def iter_orders(self) -> Iterable[OrderSnapshot]:
        return self._store.iter_orders()

    def iter_trades(self) -> Iterable[TradeSnapshot]:
        return self._store.iter_trades()

    def iter_forecasts(self) -> Iterable[ForecastSnapshot]:
        return self._store.iter_forecasts()

