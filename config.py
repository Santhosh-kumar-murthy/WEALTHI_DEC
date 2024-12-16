db_config = {
    'host': 'localhost',
    'user': 'root',
    'password': '',
    'database': 'wealthi'
}

mqtt_settings = {
    "mqtt_host": "127.0.0.1",
    "mqtt_port": 234,
    "mqtt_topic": "engine_stream",
    "mqtt_username": "root",
    "mqtt_password": "pass"
}
kite_config = {
    "user_id": "XL0940",
    "password": "pooja@#123",
    "totp": "Q2T7N3OUFNG4FXOIRDUDHHTA4QTJ2PGM"
}

observable_indices = [
    {
        "name": "NIFTY",
        "token": 256265,
        "exchange": "NSE"
    },
    {
        "name": "BANKNIFTY",
        "token": 260105,
        "exchange": "NSE"
    },
    {
        "name": "MIDCPNIFTY",
        "token": 288009,
        "exchange": "NSE"
    },
    {
        "name": "FINNIFTY",
        "token": 257801,
        "exchange": "NSE"
    },
    {
        "name": "SENSEX",
        "token": 265,
        "exchange": "BSE"
    },
    {
        "name": "BANKEX",
        "token": 274441,
        "exchange": "BSE"
    }
]
