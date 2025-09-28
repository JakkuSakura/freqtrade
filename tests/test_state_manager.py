from copy import deepcopy
from concurrent.futures import ThreadPoolExecutor
from unittest.mock import MagicMock

import pytest

from freqtrade.persistence import Order, Trade
from freqtrade.state import StateStore
from freqtrade.state.events import StateEvents
from freqtrade.oms import OrderManagementService
from tests.conftest import EXMS, create_mock_trades, get_patched_freqtradebot


def _clear_db() -> None:
    Order.session.query(Order).delete()
    Trade.session.query(Trade).delete()
    Trade.session.commit()


def test_state_manager_wallet_parity(mocker, default_conf):
    _clear_db()
    cfg = deepcopy(default_conf)
    cfg["dry_run"] = False

    mocker.patch.multiple(
        EXMS,
        get_balances=MagicMock(
            return_value={
                "USDT": {"free": 1000.0, "used": 0.0, "total": 1000.0},
                "BTC": {"free": 0.5, "used": 0.1, "total": 0.6},
            }
        ),
        fetch_positions=MagicMock(return_value=[]),
        fetch_ticker=MagicMock(return_value={"last": 1}),
        get_conversion_rate=MagicMock(return_value=1.0),
    )

    bot = get_patched_freqtradebot(mocker, cfg)

    assert bot.state_store is not None
    assert isinstance(bot.state_store, StateStore)
    assert bot.state_reader is not None

    legacy_balances = bot.wallets.export_wallet_balances()
    reader_balances = bot.state_reader.wallets()

    assert set(legacy_balances) == set(reader_balances)
    for currency, legacy_wallet in legacy_balances.items():
        snapshot = reader_balances[currency]
        assert snapshot.free == legacy_wallet.free
        assert snapshot.locked == legacy_wallet.used
        assert snapshot.total == legacy_wallet.total

    accessed = bot.wallets.get_all_balances()
    assert set(accessed) == set(legacy_balances)
    for currency, wallet in accessed.items():
        assert wallet.free == legacy_balances[currency].free
        assert wallet.used == legacy_balances[currency].used
        assert wallet.total == legacy_balances[currency].total


def test_state_manager_positions_parity(mocker, default_conf):
    _clear_db()
    cfg = deepcopy(default_conf)
    cfg["dry_run"] = False
    cfg["trading_mode"] = "futures"

    position_payload = {
        "symbol": "BTC/USDT",
        "side": "long",
        "contracts": 10,
        "initialMargin": 50.0,
        "leverage": 5,
        "unrealizedPnl": 3.5,
        "entryPrice": 25000.0,
        "notional": 250.0,
    }

    mocker.patch.multiple(
        EXMS,
        get_balances=MagicMock(return_value={"USDT": {"free": 1000.0, "used": 0.0, "total": 1000.0}}),
        fetch_positions=MagicMock(return_value=[position_payload]),
        _contracts_to_amount=MagicMock(return_value=0.01),
        fetch_ticker=MagicMock(return_value={"last": 1}),
    )

    bot = get_patched_freqtradebot(mocker, cfg)

    positions_legacy = bot.wallets.export_position_wallets()
    positions_state = bot.state_reader.positions()

    assert set(positions_legacy) == set(positions_state)
    for symbol, legacy in positions_legacy.items():
        snapshot = positions_state[symbol]
        assert snapshot.size == legacy.position
        assert snapshot.collateral == legacy.collateral
        assert snapshot.leverage == legacy.leverage
        assert snapshot.unrealized_pnl == legacy.unrealized_pnl
        assert snapshot.entry_price == (legacy.open_rate or 0.0)

    accessed = bot.wallets.get_all_positions()
    assert set(accessed) == set(positions_legacy)
    for symbol, position in accessed.items():
        legacy = positions_legacy[symbol]
        assert position.position == legacy.position
        assert position.collateral == legacy.collateral
        assert position.leverage == legacy.leverage
        assert position.unrealized_pnl == legacy.unrealized_pnl


def test_state_manager_trades_orders_parity(mocker, default_conf):
    _clear_db()
    cfg = deepcopy(default_conf)
    cfg["dry_run"] = False

    fee = MagicMock(return_value=0.001)
    create_mock_trades(fee, is_short=False, use_db=True)

    mocker.patch.multiple(
        EXMS,
        get_balances=MagicMock(return_value={"USDT": {"free": 1000.0, "used": 0.0, "total": 1000.0}}),
        fetch_positions=MagicMock(return_value=[]),
        fetch_ticker=MagicMock(return_value={"last": 1}),
        get_conversion_rate=MagicMock(return_value=1.0),
    )

    bot = get_patched_freqtradebot(mocker, cfg)

    state_trades = bot.state_reader.trades()
    open_trades = Trade.get_open_trades()
    assert state_trades
    assert open_trades
    for trade in open_trades:
        snapshot = state_trades.get(str(trade.id))
        assert snapshot is not None
        assert snapshot.symbol == trade.pair
        assert snapshot.size == trade.amount
        assert snapshot.collateral == trade.stake_amount
        assert snapshot.direction == ("short" if trade.is_short else "long")

    state_orders = bot.state_reader.orders()
    open_orders = Order.get_open_orders()
    assert state_orders
    assert open_orders
    for order in open_orders:
        snapshot = state_orders.get(order.order_id)
        assert snapshot is not None
        assert snapshot.symbol == (order.symbol or order.ft_pair)
        assert snapshot.side == (order.side or order.ft_order_side)
        assert snapshot.amount == order.safe_amount
        assert snapshot.status in {order.status, "open" if order.ft_is_open else "closed"}


def test_rpc_balance_parity_state_manager(mocker, default_conf):
    _clear_db()
    cfg = deepcopy(default_conf)
    cfg["dry_run"] = False
    cfg["stake_currency"] = "USDT"
    cfg["fiat_display_currency"] = "USD"

    mocker.patch.multiple(
        EXMS,
        get_balances=MagicMock(
            return_value={
                "USDT": {"free": 1200.0, "used": 0.0, "total": 1200.0},
                "BTC": {"free": 0.25, "used": 0.05, "total": 0.3},
            }
        ),
        fetch_positions=MagicMock(return_value=[]),
        fetch_ticker=MagicMock(return_value={"last": 1}),
        get_conversion_rate=MagicMock(return_value=1.0),
    )

    bot = get_patched_freqtradebot(mocker, cfg)

    state_res = bot.rpc._rpc_balance(cfg["stake_currency"], cfg["fiat_display_currency"])
    state_map = {item["currency"]: item for item in state_res["currencies"]}
    assert "USDT" in state_map and "BTC" in state_map

    usdt_entry = state_map["USDT"]
    assert usdt_entry["free"] == pytest.approx(1200.0)
    assert usdt_entry["balance"] == pytest.approx(1200.0)
    assert usdt_entry["used"] == pytest.approx(0.0)

    btc_entry = state_map["BTC"]
    assert btc_entry["free"] == pytest.approx(0.25)
    assert btc_entry["balance"] == pytest.approx(0.3)
    assert btc_entry["used"] == pytest.approx(0.05)

    expected_total = sum(item["est_stake"] for item in state_map.values())
    assert state_res["total"] == pytest.approx(expected_total)
    assert state_res["currencies"][0]["stake"] == cfg["stake_currency"]


def test_rpc_position_parity_state_manager(mocker, default_conf):
    _clear_db()
    cfg = deepcopy(default_conf)
    cfg["dry_run"] = False
    cfg["trading_mode"] = "futures"
    cfg["stake_currency"] = "USDT"

    position_payload = {
        "symbol": "ETH/USDT",
        "side": "short",
        "contracts": -5,
        "initialMargin": 25.0,
        "leverage": 3,
        "unrealizedPnl": -1.2,
        "entryPrice": 1800.0,
        "notional": 90.0,
    }

    mocker.patch.multiple(
        EXMS,
        get_balances=MagicMock(return_value={"USDT": {"free": 500.0, "used": 50.0, "total": 550.0}}),
        fetch_positions=MagicMock(return_value=[position_payload]),
        _contracts_to_amount=MagicMock(return_value=-0.25),
        fetch_ticker=MagicMock(return_value={"last": 1}),
    )

    bot = get_patched_freqtradebot(mocker, cfg)

    state_res = bot.rpc._rpc_position(cfg["stake_currency"], cfg.get("fiat_display_currency", ""))
    assert state_res["total_collateral"] == pytest.approx(25.0)
    assert state_res["total_unrealized_profit"] == pytest.approx(-1.2)

    state_positions = {entry["symbol"]: entry for entry in state_res["positions"]}
    assert state_positions.keys() == {"ETH/USDT"}
    position = state_positions["ETH/USDT"]
    assert position["position"] == pytest.approx(-0.25)
    assert position["collateral"] == pytest.approx(25.0)
    assert position["side"] == "short"
    assert position["leverage"] == pytest.approx(3)
    assert position["unrealized_pnl"] == pytest.approx(-1.2)


def test_rpc_trade_status_parity_state_manager(mocker, default_conf):
    _clear_db()
    cfg = deepcopy(default_conf)
    cfg["dry_run"] = False
    cfg["stake_currency"] = "USDT"
    cfg["fiat_display_currency"] = "USD"

    fee = MagicMock(return_value=0.001)
    create_mock_trades(fee, is_short=False, use_db=True)

    mocker.patch.multiple(
        EXMS,
        get_balances=MagicMock(return_value={"USDT": {"free": 800.0, "used": 0.0, "total": 800.0}}),
        fetch_positions=MagicMock(return_value=[]),
        fetch_ticker=MagicMock(return_value={"last": 1}),
        get_rate=MagicMock(return_value=0.2),
        get_conversion_rate=MagicMock(return_value=1.0),
    )

    bot = get_patched_freqtradebot(mocker, cfg)

    state_res = bot.rpc._rpc_trade_status()
    trades = {trade.id: trade for trade in Trade.get_open_trades()}
    assert len(state_res) == len(trades)
    for entry in state_res:
        trade = trades[int(entry["trade_id"])]
        assert entry["pair"] == trade.pair
        assert entry["is_open"] == trade.is_open
        assert entry["stake_amount"] == pytest.approx(trade.stake_amount)
        assert entry["amount"] == pytest.approx(trade.amount)


def test_rpc_status_table_parity_state_manager(mocker, default_conf):
    _clear_db()
    cfg = deepcopy(default_conf)
    cfg["dry_run"] = False
    cfg["stake_currency"] = "USDT"
    cfg["fiat_display_currency"] = "USD"

    fee = MagicMock(return_value=0.001)
    create_mock_trades(fee, is_short=False, use_db=True)

    mocker.patch.multiple(
        EXMS,
        get_balances=MagicMock(return_value={"USDT": {"free": 900.0, "used": 0.0, "total": 900.0}}),
        fetch_positions=MagicMock(return_value=[]),
        fetch_ticker=MagicMock(return_value={"last": 1}),
        get_rate=MagicMock(return_value=0.25),
        get_conversion_rate=MagicMock(return_value=1.0),
    )

    bot = get_patched_freqtradebot(mocker, cfg)

    state_rows, state_cols, state_profit, state_total = bot.rpc._rpc_status_table(
        cfg["stake_currency"], cfg["fiat_display_currency"]
    )
    trades = bot.rpc._rpc_trade_status()
    assert len(state_rows) == len(trades)
    assert "Pair" in state_cols
    assert isinstance(state_profit, float)
    assert isinstance(state_total, float)


def test_rpc_open_orders_parity_state_manager(mocker, default_conf):
    _clear_db()
    cfg = deepcopy(default_conf)
    cfg["dry_run"] = False
    cfg["stake_currency"] = "USDT"

    fee = MagicMock(return_value=0.001)
    create_mock_trades(fee, is_short=False, use_db=True)

    open_order_objs = Order.get_open_orders()
    fetch_orders_payload = [order.to_ccxt_object() for order in open_order_objs]

    mocker.patch.multiple(
        EXMS,
        get_balances=MagicMock(return_value={"USDT": {"free": 700.0, "used": 0.0, "total": 700.0}}),
        fetch_positions=MagicMock(return_value=[]),
        fetch_ticker=MagicMock(return_value={"last": 1}),
        fetch_open_orders=MagicMock(return_value=fetch_orders_payload),
        fetch_orders=MagicMock(return_value=fetch_orders_payload),
    )

    bot = get_patched_freqtradebot(mocker, cfg)

    state_res = bot.rpc._rpc_open_orders()
    assert state_res["order_count"] == len(open_order_objs)
    state_ids = {order["id"] for order in state_res["orders"]}
    expected_ids = {order.order_id for order in open_order_objs}
    assert state_ids == expected_ids


def test_oms_submit_order_thread_safe(default_conf, mocker):
    _clear_db()
    fee = MagicMock(return_value=0.001)
    create_mock_trades(fee, is_short=False, use_db=True)
    trade = Trade.get_open_trades()[0]

    state_events = StateEvents()
    state_store = StateStore(state_events)
    oms = OrderManagementService(state_store)
    oms.bootstrap_trades([trade])

    class DummyExchange:
        def __init__(self) -> None:
            self.counter = 0

        def create_order(
            self,
            *,
            pair: str,
            ordertype: str,
            side: str,
            amount: float,
            rate: float | None,
            reduceOnly: bool,
            time_in_force: str | None = None,
            leverage: float | None = None,
        ) -> dict[str, Any]:
            self.counter += 1
            return {
                "id": f"order_{self.counter}",
                "status": "open",
                "amount": amount,
                "filled": 0.0,
            }

    exchange = DummyExchange()

    def place_order() -> None:
        oms.submit_order(
            exchange=exchange,
            trade=trade,
            order_type="limit",
            side=trade.entry_side,
            amount=trade.amount,
            price=trade.open_rate,
            leverage=trade.leverage,
            reduce_only=False,
            time_in_force=None,
            tag="unit-test",
        )

    with ThreadPoolExecutor(max_workers=4) as executor:
        for _ in range(4):
            executor.submit(place_order)

    snapshot = oms.snapshot()
    assert len(snapshot.orders) == 4

    _clear_db()
