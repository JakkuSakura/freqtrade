"""Repository for recording finalized trade and order history."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any, ClassVar, Sequence, TYPE_CHECKING

from sqlalchemy import Float, Integer, String, select
from sqlalchemy.orm import Mapped, mapped_column

from freqtrade.persistence.base import ModelBase, SessionType
from freqtrade.util.datetime_helpers import dt_now

if TYPE_CHECKING:  # pragma: no cover
    from freqtrade.persistence.trade_model import Order, Trade


class TradeHistory(ModelBase):
    """Append-only record of finalized trades."""

    __tablename__ = "trade_history"
    session: ClassVar[SessionType | None] = None

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    trade_id: Mapped[int] = mapped_column(Integer, index=True)
    pair: Mapped[str] = mapped_column(String(25), nullable=False)
    direction: Mapped[str] = mapped_column(String(10), nullable=False)
    opened_at: Mapped[datetime] = mapped_column(nullable=False)
    closed_at: Mapped[datetime] = mapped_column(nullable=False)
    open_rate: Mapped[float] = mapped_column(Float(), nullable=False)
    close_rate: Mapped[float | None] = mapped_column(Float(), nullable=True)
    amount: Mapped[float] = mapped_column(Float(), nullable=False)
    stake_amount: Mapped[float] = mapped_column(Float(), nullable=False)
    profit_abs: Mapped[float | None] = mapped_column(Float(), nullable=True)
    profit_ratio: Mapped[float | None] = mapped_column(Float(), nullable=True)
    exit_reason: Mapped[str | None] = mapped_column(String(50), nullable=True)
    strategy: Mapped[str | None] = mapped_column(String(100), nullable=True)
    recorded_at: Mapped[datetime] = mapped_column(nullable=False, default=dt_now)


class OrderHistory(ModelBase):
    """Append-only record of order lifecycle events."""

    __tablename__ = "order_history"
    session: ClassVar[SessionType | None] = None

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    order_id: Mapped[str] = mapped_column(String(255), index=True, nullable=False)
    trade_id: Mapped[int | None] = mapped_column(Integer, index=True, nullable=True)
    symbol: Mapped[str] = mapped_column(String(25), nullable=False)
    side: Mapped[str] = mapped_column(String(10), nullable=False)
    type: Mapped[str] = mapped_column(String(25), nullable=False)
    status: Mapped[str] = mapped_column(String(25), nullable=False)
    price: Mapped[float | None] = mapped_column(Float(), nullable=True)
    amount: Mapped[float] = mapped_column(Float(), nullable=False)
    filled: Mapped[float] = mapped_column(Float(), nullable=False)
    event: Mapped[str] = mapped_column(String(25), nullable=False)
    recorded_at: Mapped[datetime] = mapped_column(nullable=False, default=dt_now)


@dataclass(slots=True)
class HistoryRepository:
    """Utility to record trade and order lifecycle events."""

    @staticmethod
    def _trade_direction(trade: "Trade") -> str:
        return "short" if trade.is_short else "long"

    @staticmethod
    def record_trade_close(trade: "Trade") -> None:
        if TradeHistory.session is None:
            return
        entry = TradeHistory(
            trade_id=trade.id,
            pair=trade.pair,
            direction=HistoryRepository._trade_direction(trade),
            opened_at=HistoryRepository._as_utc(trade.open_date_utc or trade.open_date),
            closed_at=HistoryRepository._as_utc(trade.close_date or dt_now()),
            open_rate=trade.open_rate,
            close_rate=trade.close_rate,
            amount=trade.amount,
            stake_amount=trade.stake_amount,
            profit_abs=(
                trade.close_profit_abs
                if trade.close_profit_abs is not None
                else getattr(trade, "realized_profit", 0.0)
            ),
            profit_ratio=(
                trade.close_profit
                if trade.close_profit is not None
                else getattr(trade, "profit_ratio", None)
            ),
            exit_reason=trade.exit_reason,
            strategy=trade.strategy,
            recorded_at=dt_now(),
        )
        TradeHistory.session.add(entry)

    @staticmethod
    def record_order_event(trade: "Trade | None", order: "Order", event: str) -> None:
        if OrderHistory.session is None:
            return
        entry = OrderHistory(
            order_id=str(order.order_id),
            trade_id=trade.id if trade else None,
            symbol=trade.pair if trade else order.ft_pair,
            side=order.ft_order_side,
            type=order.order_type or "unknown",
            status=order.status or ("open" if order.ft_is_open else "closed"),
            price=order.safe_price,
            amount=order.safe_amount,
            filled=order.safe_filled,
            event=event,
            recorded_at=dt_now(),
        )
        OrderHistory.session.add(entry)

    @staticmethod
    def fetch_trades(filters: Sequence[Any] | None = None) -> list[TradeHistory]:
        if TradeHistory.session is None:
            return []
        stmt = select(TradeHistory)
        if filters:
            for flt in filters:
                stmt = stmt.filter(flt)
        stmt = stmt.order_by(TradeHistory.closed_at)
        return TradeHistory.session.scalars(stmt).all()

    @staticmethod
    def fetch_order_history(trade_id: int) -> list[OrderHistory]:
        if OrderHistory.session is None:
            return []
        stmt = (
            select(OrderHistory)
            .filter(OrderHistory.trade_id == trade_id)
            .order_by(OrderHistory.recorded_at)
        )
        return OrderHistory.session.scalars(stmt).all()

    @staticmethod
    def _as_utc(dt: datetime) -> datetime:
        if dt.tzinfo is None:
            return dt.replace(tzinfo=UTC)
        return dt.astimezone(UTC)
