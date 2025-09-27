"""Helper around the schedule library to keep FreqtradeBot lean."""

from __future__ import annotations

from collections.abc import Callable
from datetime import time

from schedule import Scheduler

from freqtrade.enums import TradingMode


class BotScheduler:
    """Encapsulates recurring task scheduling for the trading bot."""

    def __init__(self, scheduler: Scheduler | None = None) -> None:
        self._scheduler = scheduler or Scheduler()

    @staticmethod
    def _format_time(hour: int, minute: int, second: int) -> str:
        return time(hour, minute, second).strftime("%H:%M:%S")

    def setup_core_jobs(
        self,
        *,
        trading_mode: TradingMode,
        refresh_positions_job: Callable[[], None],
        reset_ws_job: Callable[[], None],
        futures_maintenance_job: Callable[[], None] | None = None,
    ) -> None:
        """Register the recurring jobs the bot relies on."""

        self._scheduler.every().hour.do(refresh_positions_job)

        if trading_mode == TradingMode.FUTURES and futures_maintenance_job:
            for hour in range(24):
                for minute in (1, 31):
                    at_time = self._format_time(hour, minute, 2)
                    self._scheduler.every().day.at(at_time).do(futures_maintenance_job)

        self._scheduler.every().day.at("00:02").do(reset_ws_job)

    def run_pending(self) -> None:
        self._scheduler.run_pending()
