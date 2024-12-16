import threading

from controllers.broker_controller import BrokerController
from controllers.instruments_load_controller import InstrumentsController


def zerodha_instrument_setup():
    instrument_load_manager = InstrumentsController()
    instrument_load_manager.clear_zerodha_instruments()
    broker_controller = BrokerController()
    kite = broker_controller.kite_login()
    status, log_text = instrument_load_manager.load_zerodha_instruments(kite)


def alice_blue_instrument_setup():
    instrument_load_manager = InstrumentsController()
    instrument_load_manager.clear_alice_blue_instruments()
    status, log_text = instrument_load_manager.load_alice_blue_instruments()


def async_zerodha_instrument_setup():
    thread = threading.Thread(target=zerodha_instrument_setup)
    thread.start()


def async_alice_blue_instrument_setup():
    thread = threading.Thread(target=alice_blue_instrument_setup)
    thread.start()


async_zerodha_instrument_setup()
async_alice_blue_instrument_setup()
