"""Snapshot producers used by the DataSyncService."""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING

from freqtrade.persistence import Trade
from freqtrade.state.dtos import (
    OrderSnapshot,
    PositionSnapshot,
    RefreshMeta,
    SnapshotBundle,
    TradeSnapshot,
    WalletSnapshot,
)
from freqtrade.state.sync_service import SyncContext
from freqtrade.state.sync_service import SyncProducer
from freqtrade.util import dt_from_ts
from freqtrade.util.datetime_helpers import dt_now

if TYPE_CHECKING:  # pragma: no cover - typing helpers only
    from freqtrade.exchange import Exchange
    from freqtrade.wallets import Wallets


logger = logging.getLogger(__name__)


def make_wallets_producer(wallets: "Wallets") -> SyncProducer:
    """Return a producer that refreshes exchange balances and positions."""

    def _producer(context: SyncContext) -> SnapshotBundle:
        start = dt_now()
        wallets.update(require_update=True, publish_state=False)
        end = dt_now()
        latency = int((end - start).total_seconds() * 1000)
        meta = RefreshMeta(
            source="wallets-sync",
            fetched_at=end,
            latency_ms=latency,
            sequence=context.sequence,
        )

        wallet_snapshots = tuple(
            WalletSnapshot(
                currency=currency,
                free=wallet.free,
                locked=wallet.used,
                total=wallet.total,
                meta=meta,
            )
            for currency, wallet in wallets.export_wallet_balances().items()
        )

        position_snapshots = tuple(
            PositionSnapshot(
                symbol=symbol,
                size=position.position,
                side=position.side,
                entry_price=position.open_rate or 0.0,
                collateral=position.collateral,
                leverage=position.leverage,
                unrealized_pnl=position.unrealized_pnl,
                extra={
                    "notional_value": position.notional_value,
                },
                meta=meta,
            )
            for symbol, position in wallets.export_position_wallets().items()
        )

        return SnapshotBundle(wallets=wallet_snapshots, positions=position_snapshots)

    return _producer


def make_open_trades_producer() -> SyncProducer:
    """Return a producer that mirrors open trades into the state store."""

    def _producer(context: SyncContext) -> SnapshotBundle:
        start = dt_now()
        trades = Trade.get_open_trades()
        end = dt_now()
        latency = int((end - start).total_seconds() * 1000)
        meta = RefreshMeta(
            source="trade-sync",
            fetched_at=end,
            latency_ms=latency,
            sequence=context.sequence,
        )

        trade_snapshots = []
        for trade in trades:
            orders = tuple(order.order_id for order in trade.orders if order.order_id)
            trade_snapshots.append(
                TradeSnapshot(
                    trade_id=str(trade.id),
                    symbol=trade.pair,
                    direction="short" if trade.is_short else "long",
                    size=trade.amount,
                    entry_price=trade.open_rate or 0.0,
                    collateral=trade.stake_amount,
                    leverage=trade.leverage,
                    opened_at=trade.open_date,
                    orders=orders,
                    extra={
                        "stoploss": trade.stop_loss,
                        "strategy": trade.strategy,
                        "realized_profit": trade.realized_profit,
                        "enter_tag": trade.enter_tag,
                        "exit_reason": trade.exit_reason,
                        "nr_of_successful_entries": trade.nr_of_successful_entries,
                        "is_short": trade.is_short,
                        "trade_direction": trade.trade_direction,
                        "fee_open": trade.fee_open,
                        "fee_close": trade.fee_close,
                        "stake_currency": trade.stake_currency,
                        "stake_amount": trade.stake_amount,
                    },
                    meta=meta,
                )
            )

        return SnapshotBundle(trades=tuple(trade_snapshots))

    return _producer


def make_exchange_orders_producer(exchange: "Exchange") -> SyncProducer:
    """Return a producer that pulls open orders directly from the exchange."""

    def _producer(context: SyncContext) -> SnapshotBundle:
        start = dt_now()
        try:
            orders = exchange.fetch_open_orders([])
        except Exception as exc:  # pragma: no cover - network/ccxt issues
            logger.warning("Failed to fetch open orders: %s", exc)
            return SnapshotBundle()

        end = dt_now()
        latency = int((end - start).total_seconds() * 1000)
        meta = RefreshMeta(
            source="order-sync",
            fetched_at=end,
            latency_ms=latency,
            sequence=context.sequence,
        )

        snapshots: list[OrderSnapshot] = []
        for order in orders:
            info = order.get("info") or {}
            order_id = str(order.get("id") or info.get("orderId"))
            if not order_id:
                continue
            symbol = order.get("symbol") or info.get("symbol") or info.get("ft_pair")
            placed_at = dt_from_ts(order.get("timestamp")) if order.get("timestamp") else end
            status = order.get("status") or info.get("status") or "open"
            trade_id = info.get("ft_trade_id") or order.get("clientOrderId")
            extra = {
                "order_tag": info.get("ft_order_tag"),
                "is_entry": info.get("ft_is_entry"),
                "is_open": status.lower() not in {"closed", "canceled", "cancelled"},
                "ft_order_side": info.get("ft_order_side") or order.get("side"),
            }
            snapshots.append(
                OrderSnapshot(
                    order_id=order_id,
                    trade_id=str(trade_id) if trade_id is not None else None,
                    symbol=symbol,
                    side=order.get("side") or info.get("ft_order_side") or "buy",
                    type=order.get("type") or info.get("order_type") or "limit",
                    price=order.get("price"),
                    amount=order.get("amount") or 0.0,
                    filled=order.get("filled") or 0.0,
                    status=status,
                    placed_at=placed_at,
                    meta=meta,
                    extra=extra,
                )
            )

        return SnapshotBundle(orders=tuple(snapshots))

    return _producer
