"""Snapshot producers used by the DataSyncService."""

from __future__ import annotations

from typing import TYPE_CHECKING

from freqtrade.persistence import Order, Trade
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
from freqtrade.util.datetime_helpers import dt_now

if TYPE_CHECKING:  # pragma: no cover - typing helpers only
    from freqtrade.wallets import Wallets


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


def make_open_orders_producer() -> SyncProducer:
    """Return a producer that mirrors open orders into the state store."""

    def _producer(context: SyncContext) -> SnapshotBundle:
        start = dt_now()
        orders = Order.get_open_orders()
        end = dt_now()
        latency = int((end - start).total_seconds() * 1000)
        meta = RefreshMeta(
            source="order-sync",
            fetched_at=end,
            latency_ms=latency,
            sequence=context.sequence,
        )

        order_snapshots = []
        for order in orders:
            order_snapshots.append(
                OrderSnapshot(
                    order_id=order.order_id,
                    trade_id=order.ft_trade_id,
                    symbol=order.symbol or order.ft_pair,
                    side=order.side or order.ft_order_side,
                    type=order.order_type or "limit",
                    price=order.safe_price,
                    amount=order.safe_amount,
                    filled=order.safe_filled,
                    status=order.status or ("open" if order.ft_is_open else "closed"),
                    placed_at=order.order_date_utc,
                    meta=meta,
                    extra={
                        "order_tag": order.ft_order_tag,
                        "is_entry": order.ft_order_side == order.trade.entry_side
                        if order.trade
                        else None,
                        "is_open": order.ft_is_open,
                        "ft_order_side": order.ft_order_side,
                    },
                )
            )

        return SnapshotBundle(orders=tuple(order_snapshots))

    return _producer
