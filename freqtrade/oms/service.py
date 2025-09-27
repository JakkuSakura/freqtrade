"""Order Management Service managing live orders and trades."""

from __future__ import annotations

import logging
from dataclasses import dataclass
from datetime import UTC, datetime
from threading import RLock
from typing import TYPE_CHECKING, Any, Iterable, Mapping

from freqtrade.constants import NON_OPEN_EXCHANGE_STATES
from freqtrade.persistence import Order, Trade
from freqtrade.persistence.history_repository import HistoryRepository
from freqtrade.state import SnapshotBundle, StateStore
from freqtrade.util.datetime_helpers import dt_now

from .types import ManagedOrder, ManagedTrade, build_trade_direction

if TYPE_CHECKING:  # pragma: no cover - typing helpers only
    from freqtrade.exchange.exchange import Exchange
    from freqtrade.exchange.exchange_types import CcxtOrder

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class OMSResult:
    success: bool
    order_ids: tuple[str, ...]
    message: str = ""


class OrderManagementService:
    """Maintains an in-memory shadow of active orders/trades and publishes them to the state store."""

    def __init__(self, store: StateStore) -> None:
        self._store = store
        self._lock = RLock()
        self._orders: dict[str, ManagedOrder] = {}
        self._trades: dict[int, ManagedTrade] = {}
        self._db_trades: dict[int, Trade] = {}
        self._history = HistoryRepository

    # Public API ---------------------------------------------------------

    def bootstrap_trades(self, trades: Iterable[Trade]) -> None:
        for trade in trades:
            self.sync_trade(trade)

    def sync_trade(self, trade: Trade) -> None:
        """Refresh the managed state for a trade and its open orders."""
        with self._lock:
            if trade.is_open:
                managed_trade = self._build_trade(trade)
                self._trades[trade.id] = managed_trade
                self._db_trades[trade.id] = trade
                active_orders = set()
                for order in trade.open_orders:
                    managed_order = self._build_order(trade, order)
                    self._orders[managed_order.order_id] = managed_order
                    active_orders.add(managed_order.order_id)
                stale_orders = [
                    order_id
                    for order_id, managed_order in self._orders.items()
                    if managed_order.trade_id == trade.id and order_id not in active_orders
                ]
                for order_id in stale_orders:
                    self._orders.pop(order_id, None)
            else:
                self._trades.pop(trade.id, None)
                self._db_trades.pop(trade.id, None)
                stale_orders = [
                    order_id
                    for order_id, managed_order in self._orders.items()
                    if managed_order.trade_id == trade.id
                ]
                for order_id in stale_orders:
                    self._orders.pop(order_id, None)
        self._flush()

    def remove_trade(self, trade_id: int) -> None:
        with self._lock:
            self._trades.pop(trade_id, None)
            self._db_trades.pop(trade_id, None)
            stale_orders = [
                order_id
                for order_id, managed_order in self._orders.items()
                if managed_order.trade_id == trade_id
            ]
            for order_id in stale_orders:
                self._orders.pop(order_id, None)
        self._flush()

    def snapshot(self) -> SnapshotBundle:
        with self._lock:
            fetched_at = dt_now()
            sequence = self._store.version + 1
            order_snaps = tuple(
                order.to_snapshot(sequence, fetched_at) for order in self._orders.values()
            )
            trade_snaps = tuple(
                trade.to_snapshot(sequence, fetched_at) for trade in self._trades.values()
            )
        return SnapshotBundle(orders=order_snaps, trades=trade_snaps)

    # Lifecycle orchestration -------------------------------------------

    def register_trade(self, trade: Trade) -> None:
        """Semantic alias used when a trade is created and should be mirrored."""
        self.sync_trade(trade)

    def iter_trades(self) -> tuple[Trade, ...]:
        with self._lock:
            return tuple(self._db_trades.values())

    def get_trade(self, trade_id: int) -> Trade | None:
        with self._lock:
            return self._db_trades.get(trade_id)

    def submit_order(
        self,
        *,
        exchange: "Exchange",
        trade: Trade,
        order_type: str,
        side: str,
        amount: float,
        price: float | None,
        leverage: float | None = None,
        reduce_only: bool = False,
        time_in_force: str | None = None,
        tag: str | None = None,
    ) -> tuple[Order, "CcxtOrder"]:
        """Submit a new order via the exchange and publish it into managed state."""

        payload: dict[str, Any] = {
            "pair": trade.pair,
            "ordertype": order_type,
            "side": side,
            "amount": amount,
            "reduceOnly": reduce_only,
        }

        if price is not None:
            payload["rate"] = price
        if leverage is not None:
            payload["leverage"] = leverage
        if time_in_force:
            payload["time_in_force"] = time_in_force

        order = exchange.create_order(**payload)
        order_obj = Order.parse_from_ccxt_object(order, trade.pair, side, amount, price)
        order_obj.ft_order_tag = tag
        trade.orders.append(order_obj)

        self.sync_trade(trade)

        status = (order.get("status") or "").lower()
        if status in NON_OPEN_EXCHANGE_STATES:
            self._record_order_event(trade, order_obj, status or "closed")
            if not trade.is_open:
                self._record_trade_close(trade)

        return order_obj, order

    def submit_stoploss(
        self,
        *,
        exchange: "Exchange",
        trade: Trade,
        amount: float,
        stop_price: float,
        order_types: Mapping[str, str],
    ) -> tuple[Order, "CcxtOrder"]:
        """Create a stoploss order and mirror it into managed state."""

        order = exchange.create_stoploss(
            pair=trade.pair,
            amount=amount,
            stop_price=stop_price,
            order_types=order_types,
            side=trade.exit_side,
            leverage=trade.leverage,
        )

        order_obj = Order.parse_from_ccxt_object(
            order, trade.pair, "stoploss", amount, stop_price
        )
        order_obj.ft_order_tag = "stoploss"
        trade.orders.append(order_obj)

        self.sync_trade(trade)

        status = (order.get("status") or "").lower()
        if status in NON_OPEN_EXCHANGE_STATES:
            self._record_order_event(trade, order_obj, status or "closed")
            if not trade.is_open:
                self._record_trade_close(trade)

        return order_obj, order

    def cancel_order(
        self,
        *,
        exchange: "Exchange",
        trade: Trade,
        order: Order,
        amount: float,
        reason: str | None = None,
        is_stoploss: bool = False,
    ) -> "CcxtOrder":
        """Cancel an order and synchronize the result with managed state."""

        if is_stoploss:
            result = exchange.cancel_stoploss_order_with_result(order.order_id, trade.pair, amount)
        else:
            result = exchange.cancel_order_with_result(order.order_id, trade.pair, amount)

        if reason:
            order.ft_cancel_reason = reason

        order.update_from_ccxt_object(result)

        self.sync_trade(trade)

        event = reason or result.get("status") or "canceled"
        self._record_order_event(trade, order, event)
        if not trade.is_open:
            self._record_trade_close(trade)

        return result

    def apply_order_update(self, trade: Trade, order: Order, ccxt_order: "CcxtOrder") -> None:
        """Mirror order updates fetched from the exchange into managed caches."""

        order.update_from_ccxt_object(ccxt_order)
        self.sync_trade(trade)

        status = (ccxt_order.get("status") or "").lower()
        if status in NON_OPEN_EXCHANGE_STATES:
            self._record_order_event(trade, order, status or "closed")
            if not trade.is_open:
                self._record_trade_close(trade)

    def record_order_event(self, trade: Trade, order: Order, event: str) -> None:
        """Expose manual hooks to persist custom order lifecycle events."""
        self._record_order_event(trade, order, event)

    def archive_trade(self, trade: Trade) -> None:
        """Persist a finalized trade to the historical repository."""
        self._record_trade_close(trade)

    # Internal helpers --------------------------------------------------

    def _flush(self) -> None:
        bundle = self.snapshot()
        self._store.apply(bundle)

    def _build_order(self, trade: Trade, order) -> ManagedOrder:
        order_id = str(order.order_id or order.order_id)
        placed_at = getattr(order, "order_date", None) or dt_now()
        placed_at = placed_at if placed_at.tzinfo else placed_at.replace(tzinfo=UTC)
        updated_at = dt_now()
        return ManagedOrder(
            order_id=order_id,
            trade_id=trade.id,
            symbol=trade.pair,
            side=order.ft_order_side,
            type=order.order_type or "limit",
            price=order.safe_price,
            amount=order.safe_amount,
            filled=order.safe_filled,
            status=order.status or ("open" if order.ft_is_open else "closed"),
            placed_at=placed_at,
            updated_at=updated_at,
            extra={
                "enter_tag": trade.enter_tag,
                "exit_reason": trade.exit_reason,
                "order_tag": getattr(order, "ft_order_tag", None),
                "is_entry": order.ft_order_side == trade.entry_side,
                "is_open": order.ft_is_open,
                "ft_order_side": order.ft_order_side,
            },
        )

    def _build_trade(self, trade: Trade) -> ManagedTrade:
        direction = build_trade_direction(trade.is_short)
        orders = tuple(order.order_id for order in trade.open_orders if order.order_id)
        opened_at = trade.open_date_utc or dt_now()
        return ManagedTrade(
            trade_id=trade.id,
            symbol=trade.pair,
            direction=direction,
            size=trade.amount,
            collateral=trade.stake_amount,
            leverage=trade.leverage,
            opened_at=opened_at,
            open_rate=trade.open_rate,
            is_open=trade.is_open,
            orders=orders,
            extra={
                "enter_tag": trade.enter_tag,
                "stop_loss": trade.stop_loss,
                "strategy": trade.strategy,
                "exit_reason": trade.exit_reason,
                "nr_of_successful_entries": trade.nr_of_successful_entries,
                "is_short": trade.is_short,
                "trade_direction": trade.trade_direction,
                "fee_open": trade.fee_open,
                "fee_close": trade.fee_close,
                "stake_currency": trade.stake_currency,
                "stake_amount": trade.stake_amount,
                "realized_profit": getattr(trade, "realized_profit", None),
            },
        )

    def _record_order_event(self, trade: Trade, order: Order, event: str) -> None:
        try:
            self._history.record_order_event(trade, order, event=event)
        except Exception:  # pragma: no cover - defensive logging only
            logger.exception("Failed to record order history for %s", order.order_id)

    def _record_trade_close(self, trade: Trade) -> None:
        try:
            self._history.record_trade_close(trade)
        except Exception:  # pragma: no cover - defensive logging only
            logger.exception("Failed to archive trade %s", trade.id)
