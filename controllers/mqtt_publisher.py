import json
import logging

import paho.mqtt.client as mqtt

from config import mqtt_settings

logger = logging.getLogger(__name__)


class MqttPublisher:
    @staticmethod
    def on_publish(client, userdata, mid, a, b):
        logger.info(f"Message Published - Client: {client}, Userdata: {userdata}, MID: {mid}")

    def publish_payload(self, payload):
        try:
            mqtt_msg = json.dumps(payload, default=str)
            mqtt_host = mqtt_settings['mqtt_host']
            mqtt_port = mqtt_settings['mqtt_port']
            keep_alive_interval = 45
            mqtt_topic = mqtt_settings['mqtt_topic']
            mqtt_username = mqtt_settings["mqtt_username"]
            mqtt_password = mqtt_settings["mqtt_password"]
            mqtt_client = mqtt.Client(callback_api_version=mqtt.CallbackAPIVersion.VERSION2)
            mqtt_client.username_pw_set(mqtt_username, mqtt_password)
            mqtt_client.on_publish = self.on_publish
            mqtt_client.connect(mqtt_host, mqtt_port, keep_alive_interval)
            mqtt_client.publish(mqtt_topic, mqtt_msg)
            mqtt_client.disconnect()
            logger.info(f"Payload published successfully : {mqtt_msg}")
        except Exception as exc:
            logger.error(f"Failed to publish MQTT payload: {exc} {payload}", exc_info=True)
