#!/usr/bin/env python3
"""
Script to close all open positions on a Freqtrade account
This is useful for emergency liquidation of all positions

Usage:
python3 scripts/close_all_positions.py -c config.json
"""
import asyncio
import logging
import sys
from argparse import ArgumentParser
from datetime import UTC, datetime
from typing import Dict, List, Optional

from freqtrade.commands import Arguments
from freqtrade.configuration import setup_utils_configuration, Configuration
from freqtrade.constants import Config, BuySell
from freqtrade.enums import ExitType, RunMode, TradingMode
from freqtrade.exchange import Exchange
from freqtrade.resolvers import ExchangeResolver

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
)
logger = logging.getLogger('close_all_positions')


def setup_exchange(config: Config) -> Exchange:
    """
    Sets up the exchange with given config
    """

    # Check trading mode
    trading_mode = config.get('trading_mode', 'spot')
    if trading_mode not in ['futures', 'portfolio_margin']:
        logger.error("This script only works with futures or portfolio margin trading mode.")
        sys.exit(1)

    # Initialize exchange
    exchange = ExchangeResolver.load_exchange(config)

    # Initialize markets
    exchange.get_markets()

    return exchange


def close_all_positions(config: Config):
    """
    Closes all open positions
    """
    # Setup exchange
    exchange = setup_exchange(config)

    # Initialize wallets to get positions in a way compatible with freqtrade
    exchange.validate_stakecurrency(config.get('stake_currency', 'USDT'))

    # Use the exchange to fetch positions directly 
    positions = exchange.fetch_positions()

    # Close each position
    for position in positions:
        # Only process positions with non-zero size
        position_size = abs(position['contracts'])
        # Determine order side (opposite of position side for closing)
        if position_size <= 0:
            continue
        is_short = position['contracts'] < 0
        side: BuySell = 'buy' if is_short else 'sell'

        logger.info(
            f"Closing position for {position['symbol']}, side: {'short' if is_short else 'long'}, size: {position_size}")

        if config.get('dry_run', False):
            logger.info(f"Dry run: Would close position {position['symbol']} with {side} order of {position_size}")
        else:
            try:
                # Create market order to close position
                order = exchange.create_order(
                    pair=position['symbol'],
                    ordertype='market',
                    side=side,
                    amount=position_size,
                    rate=0,
                    leverage=1.0,
                    reduceOnly=True,
                )
                logger.info(f"Position closed with order: {order.get('id')}")
            except Exception as e:
                logger.error(f"Error closing position {position['symbol']}: {e}")


def main():
    """
    Main function
    """
    args = Arguments(sys.argv[1:]).get_parsed_arg()
    # Load configuration
    configuration = Configuration(args, RunMode.LIVE)
    config = configuration.get_config()

    # Close all positions
    close_all_positions(config)


if __name__ == "__main__":
    main()
