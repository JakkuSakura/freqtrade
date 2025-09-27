"""Order management domain models."""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import UTC, datetime
from typing import Any

from freqtrade.state import OrderSnapshot, RefreshMeta, TradeSnapshot
from freqtrade.util.datetime_helpers import dt_now


@dataclass
class ManagedOrder:
    order_id: str
    trade_id: int | None
    symbol: str
    side: str
    type: str
    price: float | None
    amount: float
    filled: float
    status: str
    placed_at: datetime
    updated_at: datetime
    extra: dict[str, Any] = field(default_factory=dict)

    def to_snapshot(self, sequence: int, fetched_at: datetime) -> OrderSnapshot:
        meta = RefreshMeta(
            source="oms",
            fetched_at=fetched_at,
            latency_ms=0,
            sequence=sequence,
        )
        return OrderSnapshot(
            order_id=self.order_id,
            trade_id=self.trade_id,
            symbol=self.symbol,
            side=self.side,
            type=self.type,
            price=self.price,
            amount=self.amount,
            filled=self.filled,
            status=self.status,
            placed_at=self.placed_at,
            meta=meta,
            extra=self.extra,
        )


@dataclass
class ManagedTrade:
    trade_id: int
    symbol: str
    direction: str
    size: float
    collateral: float
    leverage: float | None
    opened_at: datetime
    open_rate: float
    is_open: bool
    orders: tuple[str, ...]
    extra: dict[str, Any] = field(default_factory=dict)

    def to_snapshot(self, sequence: int, fetched_at: datetime) -> TradeSnapshot:
        meta = RefreshMeta(
            source="oms",
            fetched_at=fetched_at,
            latency_ms=0,
            sequence=sequence,
        )
        return TradeSnapshot(
            trade_id=str(self.trade_id),
            symbol=self.symbol,
            direction=self.direction,
            size=self.size,
            entry_price=self.open_rate,
            collateral=self.collateral,
            leverage=self.leverage,
            opened_at=self.opened_at,
            orders=self.orders,
            extra=self.extra,
            meta=meta,
        )


def build_trade_direction(is_short: bool) -> str:
    return "short" if is_short else "long"


def ensure_utc(dt: datetime) -> datetime:
    if dt.tzinfo is None:
        return dt.replace(tzinfo=UTC)
    return dt


def current_utc() -> datetime:
    return dt_now()
