import logging
from contextlib import closing

import pymysql
from pymysql.cursors import DictCursor

from config import db_config
from controllers.broker_controller import BrokerController
from controllers.mqtt_publisher import MqttPublisher

logger = logging.getLogger(__name__)


class PositionsController:
    def __init__(self):
        self.conn = pymysql.connect(**db_config, cursorclass=DictCursor)
        self.create_positions_table()
        self.mqtt_controller = MqttPublisher()

    def create_positions_table(self):
        with closing(self.conn.cursor()) as cursor:
            cursor.execute('''
                            CREATE TABLE IF NOT EXISTS idx_positions (
                                idx_position_id INT AUTO_INCREMENT PRIMARY KEY,
                                zerodha_instrument_token INT,
                                index_name VARCHAR(255),
                                exchange VARCHAR(255),
                                direction INT,
                                position_entry_time DATETIME,
                                position_entry_price FLOAT,
                                position_exit_time DATETIME,
                                position_exit_price FLOAT,
                                exit_reason VARCHAR(255),
                                profit FLOAT
                            )
                        ''')
            cursor.execute('''CREATE TABLE IF NOT EXISTS opt_positions (
                                opt_position_id INT AUTO_INCREMENT PRIMARY KEY,
                                idx_position_id INT,
                                zerodha_instrument_token INT,
                                zerodha_trading_symbol VARCHAR(255),
                                zerodha_name VARCHAR(255),
                                zerodha_exchange VARCHAR(255),
                                alice_token VARCHAR(255),
                                alice_trading_symbol VARCHAR(255),
                                alice_name VARCHAR(255),
                                alice_exchange VARCHAR(255),
                                index_name VARCHAR(255),
                                direction INT,
                                position_type INT COMMENT 
                                '1 = OPT BUY\r\n2 = OPT SELL',
                                position_entry_time DATETIME,
                                position_entry_price FLOAT,
                                position_exit_time DATETIME,
                                position_exit_price FLOAT,
                                exit_reason VARCHAR(255),
                                profit FLOAT,
                                lot_size INT,
                                expiry DATE
                            )
                    ''')
            self.conn.commit()

    def get_active_position(self, index_name):
        with closing(self.conn.cursor()) as cursor:
            cursor.execute(
                'SELECT * FROM idx_positions WHERE index_name = %s AND position_exit_time IS NULL', index_name)
            active_trade = cursor.fetchone()
        return active_trade

    def get_option_for_buying(self, index, direction, spot_price):
        instrument_types = {
            1: 'CE',
            2: 'PE'
        }
        instrument_type = instrument_types.get(direction, 'Unknown')

        queries = {
            "zerodha_long_query": """ SELECT * FROM zerodha_instruments WHERE zerodha_segment IN 
            ('NFO-OPT', 'BFO-OPT') AND zerodha_name = %s AND zerodha_instrument_type = %s AND 
            zerodha_expiry >= CURDATE() AND zerodha_strike > %s ORDER BY zerodha_expiry ASC, 
            zerodha_strike ASC LIMIT 1; """,
            "zerodha_short_query": """ SELECT * FROM zerodha_instruments WHERE zerodha_segment IN 
            ('NFO-OPT', 'BFO-OPT') AND zerodha_name = %s AND zerodha_instrument_type = %s AND 
            zerodha_expiry >= CURDATE() AND zerodha_strike < %s ORDER BY zerodha_expiry ASC, zerodha_strike 
            DESC LIMIT 1; """,
            "alice_long_query": """SELECT * FROM alice_blue_instruments WHERE alice_instrument_type IN ('OPTIDX', 'IO') 
            AND alice_symbol = %s AND alice_expiry_date >= CURDATE() AND alice_strike_price > %s AND 
            alice_option_type = %s ORDER BY alice_expiry_date ASC, alice_strike_price ASC LIMIT 1;""",
            "alice_short_query": """SELECT * FROM alice_blue_instruments WHERE alice_instrument_type IN ('OPTIDX', 'IO')
            AND alice_symbol = %s AND alice_expiry_date >= CURDATE() AND alice_strike_price < %s AND 
            alice_option_type = %s ORDER BY alice_expiry_date ASC, alice_strike_price DESC LIMIT 1;"""
        }
        zerodha_query = queries.get('zerodha_long_query' if direction == 1 else 'zerodha_short_query', 'Unknown')
        alice_query = queries.get('alice_long_query' if direction == 1 else 'alice_short_query', 'Unknown')

        with closing(self.conn.cursor()) as cursor:
            cursor.execute(zerodha_query,
                           (index['name'], instrument_type, spot_price))
            zerodha_option = cursor.fetchone()

            cursor.execute(alice_query, (index['name'], str(spot_price), instrument_type))
            alice_option = cursor.fetchone()
            return {
                "zerodha_option": zerodha_option,
                "alice_option": alice_option
            }

    def get_option_for_selling(self, index, direction, spot_price):
        instrument_types = {
            1: 'PE',
            2: 'CE'
        }
        instrument_type = instrument_types.get(direction, 'Unknown')

        queries = {
            "zerodha_long_query": """ SELECT * FROM zerodha_instruments WHERE zerodha_segment IN 
            ('NFO-OPT', 'BFO-OPT') AND zerodha_name = %s AND zerodha_instrument_type = %s AND 
            zerodha_expiry >= CURDATE() AND zerodha_strike > %s ORDER BY zerodha_expiry ASC, zerodha_strike 
            ASC LIMIT 1; """,
            "zerodha_short_query": """ SELECT * FROM zerodha_instruments WHERE zerodha_segment IN 
            ('NFO-OPT', 'BFO-OPT') AND zerodha_name = %s AND zerodha_instrument_type = %s AND 
            zerodha_expiry >= CURDATE() AND zerodha_strike < %s ORDER BY zerodha_expiry ASC, zerodha_strike 
            DESC LIMIT 1; """,
            "alice_long_query": """SELECT * FROM alice_blue_instruments WHERE alice_instrument_type IN ('OPTIDX', 'IO')  
            AND alice_symbol = %s AND alice_expiry_date >= CURDATE() AND alice_strike_price > %s 
            AND alice_option_type = %s ORDER BY alice_expiry_date ASC, alice_strike_price ASC LIMIT 1;""",
            "alice_short_query": """SELECT * FROM alice_blue_instruments WHERE alice_instrument_type IN ('OPTIDX', 'IO')
            AND alice_symbol = %s AND alice_expiry_date >= CURDATE() AND alice_strike_price < %s AND 
            alice_option_type = %s ORDER BY alice_expiry_date ASC, alice_strike_price DESC LIMIT 1;"""
        }

        zerodha_query = queries.get('zerodha_long_query' if direction == 2 else 'zerodha_short_query', 'Unknown')
        alice_query = queries.get('alice_long_query' if direction == 2 else 'alice_short_query', 'Unknown')
        with closing(self.conn.cursor()) as cursor:
            cursor.execute(zerodha_query,
                           (index['name'], instrument_type, spot_price))
            zerodha_option = cursor.fetchone()

            cursor.execute(alice_query, (index['name'], str(spot_price), instrument_type))
            alice_option = cursor.fetchone()
            return {
                "zerodha_option": zerodha_option,
                "alice_option": alice_option
            }

    def make_new_position(self, index, close, direction, kite):
        with self.conn.cursor() as cursor:
            # Insert the new index position
            cursor.execute('''
                INSERT INTO idx_positions (
                    zerodha_instrument_token, index_name, exchange, direction,
                    position_entry_time, position_entry_price
                ) VALUES (%s, %s, %s, %s, NOW(), %s)
            ''', (index['token'], index['name'], index['exchange'], direction, close))
            idx_position_id = cursor.lastrowid  # Retrieve the ID of the newly created position

        self.conn.commit()

        # Get buy and sell option data
        buy_option_data = self.get_option_for_buying(
            index=index, direction=direction, spot_price=close
        )
        sell_option_data = self.get_option_for_selling(
            index=index, direction=direction, spot_price=close
        )

        broker_controller = BrokerController()

        # Fetch entry prices for Zerodha options
        if buy_option_data.get('zerodha_option'):
            buy_option_data['entry_price'] = broker_controller.get_ltp_kite(
                kite, buy_option_data['zerodha_option']['zerodha_instrument_token']
            )
        if sell_option_data.get('zerodha_option'):
            sell_option_data['entry_price'] = broker_controller.get_ltp_kite(
                kite, sell_option_data['zerodha_option']['zerodha_instrument_token']
            )

        # Insert buy option data into the opt_positions table
        if buy_option_data.get('zerodha_option') or buy_option_data.get('alice_option'):
            with self.conn.cursor() as cursor:
                cursor.execute('''
                    INSERT INTO opt_positions (
                        idx_position_id, zerodha_instrument_token, zerodha_trading_symbol,
                        zerodha_name, zerodha_exchange, alice_token, alice_trading_symbol,
                        alice_name, alice_exchange, index_name, direction,
                        position_type, position_entry_time, position_entry_price, lot_size, expiry
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, 1, NOW(), %s, %s, %s)
                ''', (
                    idx_position_id,
                    buy_option_data['zerodha_option']['zerodha_instrument_token'] if buy_option_data.get(
                        'zerodha_option') else None,
                    buy_option_data['zerodha_option']['zerodha_trading_symbol'] if buy_option_data.get(
                        'zerodha_option') else None,
                    buy_option_data['zerodha_option']['zerodha_name'] if buy_option_data.get(
                        'zerodha_option') else None,
                    buy_option_data['zerodha_option']['zerodha_exchange'] if buy_option_data.get(
                        'zerodha_option') else None,
                    buy_option_data['alice_option']['alice_token'] if buy_option_data.get('alice_option') else None,
                    buy_option_data['alice_option']['alice_trading_symbol'] if buy_option_data.get(
                        'alice_option') else None,
                    buy_option_data['alice_option']['alice_symbol'] if buy_option_data.get('alice_option') else None,
                    buy_option_data['alice_option']['alice_exchange'] if buy_option_data.get('alice_option') else None,
                    index['name'],
                    direction,
                    buy_option_data.get('entry_price'),
                    buy_option_data['zerodha_option']['zerodha_lot_size'] if buy_option_data.get(
                        'zerodha_option') else None,
                    buy_option_data['zerodha_option']['zerodha_expiry'] if buy_option_data.get(
                        'zerodha_option') else None
                ))

        # Insert sell option data into the opt_positions table
        if sell_option_data.get('zerodha_option') or sell_option_data.get('alice_option'):
            with self.conn.cursor() as cursor:
                cursor.execute('''
                    INSERT INTO opt_positions (
                        idx_position_id, zerodha_instrument_token, zerodha_trading_symbol,
                        zerodha_name, zerodha_exchange, alice_token, alice_trading_symbol,
                        alice_name, alice_exchange, index_name, direction,
                        position_type, position_entry_time, position_entry_price, lot_size, expiry
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, 2, NOW(), %s, %s, %s)
                ''', (
                    idx_position_id,
                    sell_option_data['zerodha_option']['zerodha_instrument_token'] if sell_option_data.get(
                        'zerodha_option') else None,
                    sell_option_data['zerodha_option']['zerodha_trading_symbol'] if sell_option_data.get(
                        'zerodha_option') else None,
                    sell_option_data['zerodha_option']['zerodha_name'] if sell_option_data.get(
                        'zerodha_option') else None,
                    sell_option_data['zerodha_option']['zerodha_exchange'] if sell_option_data.get(
                        'zerodha_option') else None,
                    sell_option_data['alice_option']['alice_token'] if sell_option_data.get('alice_option') else None,
                    sell_option_data['alice_option']['alice_trading_symbol'] if sell_option_data.get(
                        'alice_option') else None,
                    sell_option_data['alice_option']['alice_symbol'] if sell_option_data.get('alice_option') else None,
                    sell_option_data['alice_option']['alice_exchange'] if sell_option_data.get(
                        'alice_option') else None,
                    index['name'],
                    direction,
                    sell_option_data.get('entry_price'),
                    sell_option_data['zerodha_option']['zerodha_lot_size'] if sell_option_data.get(
                        'zerodha_option') else None,
                    sell_option_data['zerodha_option']['zerodha_expiry'] if sell_option_data.get(
                        'zerodha_option') else None
                ))

        self.conn.commit()

        # Publish the payload via MQTT
        self.mqtt_controller.publish_payload(
            {
                "signal_type": "ENTRY",
                "index_name": index['name'],
                "exchange": index['exchange'],
                "direction": direction,
                "entry_price": close,
                "buy_option_data": buy_option_data,
                "sell_option_data": sell_option_data
            }
        )

    def exit_position(self, active_position, exit_price, kite, exit_reason="Strategy Exit"):
        if not active_position:
            logger.debug("No active position to exit.")
            return

        position_id = active_position['idx_position_id']
        entry_price = active_position['position_entry_price']
        direction = active_position['direction']

        # Calculate profit for the index position
        if direction == 1:
            profit = exit_price - entry_price
        elif direction == 2:
            profit = entry_price - exit_price
        else:
            profit = 0

        with self.conn.cursor() as cursor:
            # Update the index position
            cursor.execute('''
                UPDATE idx_positions
                SET position_exit_time = NOW(),
                    position_exit_price = %s,
                    exit_reason = %s,
                    profit = %s
                WHERE idx_position_id = %s
            ''', (exit_price, exit_reason, profit, position_id))

            # Fetch associated options
            cursor.execute('''
                SELECT * FROM opt_positions WHERE idx_position_id = %s AND position_exit_time IS NULL
            ''', (position_id,))
            options = cursor.fetchall()

            broker_controller = BrokerController()

            # Exit options
            options_exit_details = []
            for option in options:
                option_id = option['opt_position_id']
                option_entry_price = option['position_entry_price']
                option_direction = option['direction']
                option_type = option['position_type']

                # Get live trading price for the option
                opt_exit_price = broker_controller.get_ltp_kite(kite, option['zerodha_instrument_token'])

                # Determine profit/loss for the option
                if option_type == 1:  # Buy option
                    option_profit = opt_exit_price - option_entry_price if option_direction == 1 else option_entry_price - opt_exit_price
                elif option_type == 2:  # Sell option
                    option_profit = option_entry_price - opt_exit_price if option_direction == 1 else opt_exit_price - option_entry_price
                else:
                    option_profit = 0

                # Update the option position
                cursor.execute('''
                    UPDATE opt_positions
                    SET position_exit_time = NOW(),
                        position_exit_price = %s,
                        exit_reason = %s,
                        profit = %s
                    WHERE opt_position_id = %s
                ''', (opt_exit_price, exit_reason, option_profit, option_id))

                # Collect option exit details for MQTT
                options_exit_details.append({
                    "option": option,
                    "exit_price": opt_exit_price,
                    "profit": option_profit
                })

        self.conn.commit()

        # Publish exit details via MQTT
        self.mqtt_controller.publish_payload(
            {
                "signal_type": "EXIT",
                "index_name": active_position['index_name'],
                "exchange": active_position["exchange"],
                "exit_reason": exit_reason,
                "exit_price": exit_price,  # Keep the index exit price consistent
                "profit": profit,
                "options_exit": options_exit_details
            }
        )
