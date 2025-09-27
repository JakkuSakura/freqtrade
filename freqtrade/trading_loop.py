"""Trading loop orchestration extracted from FreqtradeBot."""

from __future__ import annotations

from datetime import UTC, datetime
from typing import TYPE_CHECKING

from freqtrade.strategy.strategy_wrapper import strategy_safe_wrapper
from freqtrade.persistence import Trade
from freqtrade.enums import State


if TYPE_CHECKING:  # pragma: no cover
    from freqtrade.freqtradebot import FreqtradeBot


class TradingLoop:
    """Runs a full trading iteration for the bot."""

    def __init__(self, bot: "FreqtradeBot") -> None:
        self._bot = bot

    def run_cycle(self) -> None:
        bot = self._bot

        bot.exchange.reload_markets()
        bot.update_trades_without_assigned_fees()

        trades: list[Trade] = Trade.get_open_trades()

        bot.active_pair_whitelist = bot._refresh_active_whitelist(trades)

        bot.dataprovider.refresh(
            bot.pairlists.create_pair_list(bot.active_pair_whitelist),
            bot.strategy.gather_informative_pairs(),
        )

        strategy_safe_wrapper(bot.strategy.bot_loop_start, supress_error=True)(
            current_time=datetime.now(UTC)
        )

        with bot._measure_execution:
            bot.strategy.analyze(bot.active_pair_whitelist)

        with bot._exit_lock:
            bot.manage_open_orders()

        with bot._exit_lock:
            trades = Trade.get_open_trades()
            bot.exit_positions(trades)
            Trade.commit()

        if bot.strategy.position_adjustment_enable:
            with bot._exit_lock:
                bot.process_open_trade_positions()

        if bot.state == State.RUNNING and bot.get_free_open_trades():
            bot.enter_positions()

        bot._scheduler.run_pending()
        Trade.commit()
        bot.rpc.process_msg_queue(bot.dataprovider._msg_queue)
        bot.last_process = datetime.now(UTC)
