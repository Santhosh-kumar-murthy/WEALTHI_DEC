import datetime
import pymysql
import requests
from pymysql.cursors import DictCursor

from config import db_config


class InstrumentsController:
    def __init__(self):
        self.conn = pymysql.connect(**db_config, cursorclass=DictCursor)
        self.create_zerodha_instruments_table()
        self.create_alice_blue_instruments_table()

    def create_zerodha_instruments_table(self):
        with self.conn.cursor() as cursor:
            cursor.execute('''
                            CREATE TABLE IF NOT EXISTS zerodha_instruments (
                                zerodha_instrument_token INT,
                                zerodha_exchange_token INT,
                                zerodha_trading_symbol VARCHAR(255),
                                zerodha_name VARCHAR(255),
                                zerodha_last_price FLOAT,
                                zerodha_expiry DATE,
                                zerodha_strike FLOAT,
                                zerodha_tick_size FLOAT,
                                zerodha_lot_size INT,
                                zerodha_instrument_type VARCHAR(255),
                                zerodha_segment VARCHAR(255),
                                zerodha_exchange VARCHAR(255)
                            )
                        ''')
            self.conn.commit()

    def create_alice_blue_instruments_table(self):
        with self.conn.cursor() as cursor:
            cursor.execute('''
                            CREATE TABLE IF NOT EXISTS alice_blue_instruments (
                                alice_exchange VARCHAR(255),
                                alice_exchange_segment VARCHAR(255),
                                alice_expiry_date DATE,
                                alice_formatted_ins_name VARCHAR(255),
                                alice_instrument_type VARCHAR(255),
                                alice_lot_size INT,
                                alice_option_type VARCHAR(255),
                                alice_pdc DECIMAL(10, 2) NULL,
                                alice_strike_price DECIMAL(10, 2),
                                alice_symbol VARCHAR(255),
                                alice_tick_size VARCHAR(255),
                                alice_token VARCHAR(255),
                                alice_trading_symbol VARCHAR(255)
                            )
                        ''')
            self.conn.commit()

    def clear_zerodha_instruments(self):
        with self.conn.cursor() as cursor:
            cursor.execute('''TRUNCATE TABLE zerodha_instruments''')
            self.conn.commit()

    def clear_alice_blue_instruments(self):
        with self.conn.cursor() as cursor:
            cursor.execute('''TRUNCATE TABLE alice_blue_instruments''')
            self.conn.commit()

    def load_zerodha_instruments(self, kite):
        try:
            all_instruments = kite.instruments()
            insert_query = """INSERT INTO zerodha_instruments (zerodha_instrument_token, zerodha_exchange_token, 
            zerodha_trading_symbol, zerodha_name, zerodha_last_price, zerodha_expiry, zerodha_strike, zerodha_tick_size,
             zerodha_lot_size, zerodha_instrument_type, zerodha_segment, zerodha_exchange ) 
             VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"""
            with self.conn.cursor() as cursor:
                for instrument in all_instruments:
                    data = (
                        instrument['instrument_token'],
                        instrument['exchange_token'],
                        instrument['tradingsymbol'],
                        instrument['name'],
                        instrument['last_price'],
                        instrument['expiry'],
                        instrument['strike'],
                        instrument['tick_size'],
                        instrument['lot_size'],
                        instrument['instrument_type'],
                        instrument['segment'],
                        instrument['exchange']
                    )
                    cursor.execute(insert_query, data)
                    self.conn.commit()
            return True, "Zerodha instruments load successful"
        except Exception as e:
            return False, str(e)

    def load_alice_blue_instruments(self):
        try:
            base_url = "https://v2api.aliceblueonline.com/restpy/contract_master?exch="
            master_instruments_segments = [
                "NSE",
                "NFO",
                "BSE",
                "BFO",
                "INDICES"
            ]
            for segment in master_instruments_segments:
                response = requests.get(base_url + segment)
                instruments_data = response.json()

                # Check if the segment key is present in the response
                if segment not in instruments_data:
                    continue

                with self.conn.cursor() as cursor:
                    insert_query = """
                        INSERT INTO alice_blue_instruments 
                        (alice_exchange, alice_exchange_segment, alice_expiry_date, alice_formatted_ins_name, 
                        alice_instrument_type, alice_lot_size, alice_option_type, alice_pdc, alice_strike_price, 
                        alice_symbol, alice_tick_size, alice_token, alice_trading_symbol) 
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);"""

                    for instrument in instruments_data[segment]:
                        expiry_date = None

                        if "expiry_date" in instrument and instrument["expiry_date"]:
                            try:
                                expiry_date = datetime.datetime.fromtimestamp(
                                    instrument["expiry_date"] / 1000
                                ).strftime("%Y-%m-%d")
                            except (ValueError, TypeError):
                                expiry_date = None

                        cursor.execute(
                            insert_query,
                            (
                                instrument.get('exch'),
                                instrument.get("exchange_segment"),
                                expiry_date,
                                instrument.get('formatted_ins_name'),
                                instrument.get('instrument_type'),
                                instrument.get('lot_size'),
                                instrument.get('option_type', 'XX'),
                                # Default value 'XX' for option type if not present
                                instrument.get('pdc'),
                                instrument.get('strike_price'),
                                instrument.get('symbol'),
                                instrument.get('tick_size'),
                                instrument.get('token'),
                                instrument.get('trading_symbol')
                            )
                        )
            self.conn.commit()
            return True, "Alice Blue instruments load successful"
        except Exception as e:
            return False, str(e)
