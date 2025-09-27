"""Typed DTOs for state snapshots maintained in memory."""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, Mapping, Sequence


@dataclass(frozen=True)
class RefreshMeta:
    """Metadata describing when and how a snapshot was produced."""

    source: str
    fetched_at: datetime
    latency_ms: int
    sequence: int


@dataclass(frozen=True)
class WalletSnapshot:
    """Wallet view normalized for fast read access."""

    currency: str
    free: float
    locked: float
    total: float
    meta: RefreshMeta


@dataclass(frozen=True)
class PositionSnapshot:
    """Exchange position information used by trading logic."""

    symbol: str
    size: float
    side: str
    entry_price: float
    collateral: float
    leverage: float | None
    unrealized_pnl: float
    extra: Mapping[str, Any]
    meta: RefreshMeta


@dataclass(frozen=True)
class OrderSnapshot:
    """Live order representation owned by the OMS."""

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
    meta: RefreshMeta
    extra: Mapping[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class TradeSnapshot:
    """Live trade representation that mirrors position lifecycle."""

    trade_id: str
    symbol: str
    direction: str
    size: float
    entry_price: float
    collateral: float
    leverage: float | None
    opened_at: datetime
    orders: Sequence[str] = field(default_factory=tuple)
    extra: Mapping[str, Any] = field(default_factory=dict)
    meta: RefreshMeta = field(default_factory=lambda: RefreshMeta(
        source="unknown",
        fetched_at=datetime.min,
        latency_ms=0,
        sequence=0,
    ))


@dataclass(frozen=True)
class ForecastSnapshot:
    """Predictive signals or derived metrics associated with a symbol."""

    key: str
    symbol: str | None
    value: Any
    meta: RefreshMeta


@dataclass(frozen=True)
class SnapshotBundle:
    """Atomic bundle of updated snapshots applied to the store."""

    wallets: Sequence[WalletSnapshot] | None = None
    positions: Sequence[PositionSnapshot] | None = None
    orders: Sequence[OrderSnapshot] | None = None
    trades: Sequence[TradeSnapshot] | None = None
    forecasts: Sequence[ForecastSnapshot] | None = None
