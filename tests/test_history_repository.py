from copy import deepcopy
from unittest.mock import MagicMock

from sqlalchemy import delete, select

from freqtrade.persistence.history_repository import HistoryRepository, OrderHistory, TradeHistory
from freqtrade.persistence.trade_model import Order, Trade
from freqtrade.util.datetime_helpers import dt_now
from tests.conftest import EXMS, create_mock_trades, get_patched_freqtradebot


def test_history_repository_records_trade_close(mocker, default_conf_usdt, fee, limit_order) -> None:
    cfg = deepcopy(default_conf_usdt)
    cfg["dry_run"] = False
    mocker.patch.multiple(
        EXMS,
        get_balances=MagicMock(return_value={"USDT": {"free": 1000.0, "used": 0.0, "total": 1000.0}}),
        fetch_positions=MagicMock(return_value=[]),
        fetch_ticker=MagicMock(return_value={"last": 1}),
        create_order=MagicMock(return_value=limit_order["buy"]),
        get_fee=fee,
    )

    get_patched_freqtradebot(mocker, cfg)

    TradeHistory.session.execute(delete(TradeHistory))
    TradeHistory.session.execute(delete(OrderHistory))
    TradeHistory.session.commit()

    create_mock_trades(fee, is_short=False, use_db=True)
    trade = Trade.get_open_trades()[0]

    trade.is_open = False
    trade.close_date = dt_now()
    trade.close_rate = trade.open_rate * 1.05
    trade.close_profit = 0.05
    trade.close_profit_abs = trade.stake_amount * 0.05
    trade.exit_reason = "unit-test"

    HistoryRepository.record_trade_close(trade)
    Trade.commit()

    entries = TradeHistory.session.scalars(select(TradeHistory)).all()
    assert len(entries) == 1
    history = entries[0]
    assert history.trade_id == trade.id
    assert history.exit_reason == "unit-test"
    assert history.direction == ("short" if trade.is_short else "long")

    order = trade.orders[0]
    HistoryRepository.record_order_event(trade, order, event="closed")
    Trade.commit()

    order_entries = OrderHistory.session.scalars(select(OrderHistory)).all()
    assert len(order_entries) == 1
    order_history = order_entries[0]
    assert order_history.order_id == order.order_id
    assert order_history.event == "closed"
