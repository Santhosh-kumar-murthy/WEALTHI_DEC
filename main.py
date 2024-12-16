from datetime import time, datetime
import logging

from config import observable_indices
from controllers.broker_controller import BrokerController
from controllers.positions_controller import PositionsController
from controllers.technical_analysis import TechnicalAnalysis

broker_controller = BrokerController()
technical_analysis = TechnicalAnalysis()
positions_controller = PositionsController()

logging.basicConfig(
    filename='trading_engine.log',
    level=logging.DEBUG,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

TRADING_END_TIME = time(15, 15)
kite = broker_controller.kite_login()

if __name__ == '__main__':
    while True:
        try:
            current_time = datetime.now().time()
            if current_time > TRADING_END_TIME:
                for index in observable_indices:
                    active_position = positions_controller.get_active_position(index['name'])
                    if active_position:
                        exit_price = broker_controller.get_ltp_kite(kite, index['token'])
                        positions_controller.exit_position(active_position, exit_price, kite, exit_reason="End of Day")
                break
            for index in observable_indices:
                historic_1min = broker_controller.kite_historic_data(kite, index['token'], 'minute')
                historic_5min = broker_controller.kite_historic_data(kite, index['token'], '5minute')
                historic_15min = broker_controller.kite_historic_data(kite, index['token'], '15minute')
                applied_df_1min = technical_analysis.calculate_signals(historic_1min)
                applied_df_5min = technical_analysis.calculate_signals(historic_5min)
                applied_df_15min = technical_analysis.calculate_signals(historic_15min)
                active_position = positions_controller.get_active_position(index['name'])
                if active_position and active_position['direction'] == 1:
                    if applied_df_1min.iloc[-2].sell_signal and applied_df_5min.iloc[-1].sell_signal:
                        positions_controller.exit_position(active_position, applied_df_1min.iloc[-1].close, kite)
                if active_position and active_position['direction'] == 2:
                    if applied_df_1min.iloc[-2].buy_signal and applied_df_5min.iloc[-1].buy_signal:
                        positions_controller.exit_position(active_position, applied_df_1min.iloc[-1].close, kite)
                active_position = positions_controller.get_active_position(index['name'])
                if applied_df_1min.iloc[-2].buy_signal and applied_df_5min.iloc[-1].buy_signal and \
                        applied_df_15min.iloc[-1].buy_signal:
                    if active_position and active_position['direction'] != 1:
                        positions_controller.exit_position(active_position, applied_df_1min.iloc[-1].close, kite)
                    if not active_position or active_position['direction'] != 1:
                        positions_controller.make_new_position(index, applied_df_1min.iloc[-1].close, 1, kite)
                if applied_df_1min.iloc[-2].sell_signal and applied_df_5min.iloc[-1].sell_signal and \
                        applied_df_15min.iloc[-1].sell_signal:
                    if active_position and active_position['direction'] != 2:
                        positions_controller.exit_position(active_position, applied_df_1min.iloc[-1].close, kite)
                    if not active_position or active_position['direction'] != 2:
                        positions_controller.make_new_position(index, applied_df_1min.iloc[-1].close, 2, kite)
        except Exception as e:
            logging.error(f"MAIN_FUNCTION ERROR: {e}", exc_info=True)
