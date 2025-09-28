"""RPC manager wiring plus log-forwarding handler."""

import logging
import traceback
from collections import deque

from freqtrade.constants import Config
from freqtrade.enums import NO_ECHO_MESSAGES, RPCMessageType
from freqtrade.rpc import RPC, RPCHandler
from freqtrade.rpc.rpc_types import RPCSendMsg


logger = logging.getLogger(__name__)


class _RPCLogHandler(logging.Handler):
    """Forward WARNING+/ERROR logs to RPC listeners (e.g., Telegram)."""

    def __init__(self, manager: "RPCManager") -> None:
        super().__init__(level=logging.WARNING)
        self._manager = manager

    def emit(self, record: logging.LogRecord) -> None:
        # Avoid loops by skipping RPC's own logs
        if record.name.startswith("freqtrade.rpc"):
            return

        try:
            status = f"{record.levelname} - {record.name}: {record.getMessage()}"
            if record.exc_info:
                exc_text = "".join(traceback.format_exception(*record.exc_info)).strip()
                status = f"{status}\n```{exc_text}```"

            msg_type = (
                RPCMessageType.EXCEPTION
                if record.levelno >= logging.ERROR
                else RPCMessageType.WARNING
            )

            self._manager.send_msg({"type": msg_type, "status": status})
        except Exception:  # pragma: no cover - defensive
            # Never let logging handlers raise
            logging.getLogger(__name__).exception("Failed to emit RPC log message")


class RPCManager:
    """
    Class to manage RPC objects (Telegram, API, ...)
    """

    def __init__(self, freqtrade) -> None:
        """Initializes all enabled rpc modules"""
        self.registered_modules: list[RPCHandler] = []
        self._rpc = RPC(freqtrade)
        self._log_handler: _RPCLogHandler | None = None
        config = freqtrade.config
        # Enable telegram
        if config.get("telegram", {}).get("enabled", False):
            logger.info("Enabling rpc.telegram ...")
            from freqtrade.rpc.telegram import Telegram

            self.registered_modules.append(Telegram(self._rpc, config))

        # Enable discord
        if config.get("discord", {}).get("enabled", False):
            logger.info("Enabling rpc.discord ...")
            from freqtrade.rpc.discord import Discord

            self.registered_modules.append(Discord(self._rpc, config))

        # Enable Webhook
        if config.get("webhook", {}).get("enabled", False):
            logger.info("Enabling rpc.webhook ...")
            from freqtrade.rpc.webhook import Webhook

            self.registered_modules.append(Webhook(self._rpc, config))

        # Enable local rest api server for cmd line control
        if config.get("api_server", {}).get("enabled", False):
            logger.info("Enabling rpc.api_server")
            from freqtrade.rpc.api_server import ApiServer

            apiserver = ApiServer(config)
            apiserver.add_rpc_handler(self._rpc)
            self.registered_modules.append(apiserver)

        # Forward WARNING/ERROR logs to RPC recipients (Telegram, etc.)
        self._log_handler = _RPCLogHandler(self)
        logging.getLogger().addHandler(self._log_handler)

    def cleanup(self) -> None:
        """Stops all enabled rpc modules"""
        logger.info("Cleaning up rpc modules ...")
        if self._log_handler:
            logging.getLogger().removeHandler(self._log_handler)
            self._log_handler = None
        while self.registered_modules:
            mod = self.registered_modules.pop()
            logger.info("Cleaning up rpc.%s ...", mod.name)
            mod.cleanup()
            del mod

    def send_msg(self, msg: RPCSendMsg) -> None:
        """
        Send given message to all registered rpc modules.
        A message consists of one or more key value pairs of strings.
        e.g.:
        {
            'status': 'stopping bot'
        }
        """
        if msg.get("type") not in NO_ECHO_MESSAGES:
            logger.info("Sending rpc message: %s", msg)
        for mod in self.registered_modules:
            logger.debug("Forwarding message to rpc.%s", mod.name)
            try:
                mod.send_msg(msg)
            except NotImplementedError:
                logger.error(f"Message type '{msg['type']}' not implemented by handler {mod.name}.")
            except Exception:
                logger.exception("Exception occurred within RPC module %s", mod.name)

    def process_msg_queue(self, queue: deque) -> None:
        """
        Process all messages in the queue.
        """
        while queue:
            msg = queue.popleft()
            logger.info("Sending rpc strategy_msg: %s", msg)
            for mod in self.registered_modules:
                if mod._config.get(mod.name, {}).get("allow_custom_messages", False):
                    mod.send_msg(
                        {
                            "type": RPCMessageType.STRATEGY_MSG,
                            "msg": msg,
                        }
                    )

    def startup_messages(self, config: Config, pairlist, protections) -> None:
        if config["dry_run"]:
            self.send_msg(
                {
                    "type": RPCMessageType.WARNING,
                    "status": "Dry run is enabled. All trades are simulated.",
                }
            )
        stake_currency = config["stake_currency"]
        stake_amount = config["stake_amount"]
        minimal_roi = config["minimal_roi"]
        stoploss = config["stoploss"]
        trailing_stop = config["trailing_stop"]
        timeframe = config["timeframe"]
        exchange_name = config["exchange"]["name"]
        strategy_name = config.get("strategy", "")
        pos_adjust_enabled = "On" if config["position_adjustment_enable"] else "Off"
        self.send_msg(
            {
                "type": RPCMessageType.STARTUP,
                "status": f"*Exchange:* `{exchange_name}`\n"
                f"*Stake per trade:* `{stake_amount} {stake_currency}`\n"
                f"*Minimum ROI:* `{minimal_roi}`\n"
                f"*{'Trailing ' if trailing_stop else ''}Stoploss:* `{stoploss}`\n"
                f"*Position adjustment:* `{pos_adjust_enabled}`\n"
                f"*Timeframe:* `{timeframe}`\n"
                f"*Strategy:* `{strategy_name}`",
            }
        )
        self.send_msg(
            {
                "type": RPCMessageType.STARTUP,
                "status": f"Searching for {stake_currency} pairs to buy and sell "
                f"based on {pairlist.short_desc()}",
            }
        )
        if len(protections.name_list) > 0:
            prots = "\n".join([p for prot in protections.short_desc() for k, p in prot.items()])
            self.send_msg(
                {"type": RPCMessageType.STARTUP, "status": f"Using Protections: \n{prots}"}
            )
