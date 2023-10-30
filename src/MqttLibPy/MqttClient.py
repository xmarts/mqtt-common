import json

from paho.mqtt.publish import single
from paho.mqtt.client import MQTTv5, Client

from logging import getLogger
from typing import Union

from .serializer import Serializer


class MqttClient:

    def __init__(self, postfix: str, hostname: str, port: str):
        self.server = postfix
        self.hostname = hostname
        self.port = port

        self.routes = []

        self.client = Client("", userdata=None, protocol=MQTTv5)

        def _on_connect(client: Client, _, __, ___, ____):
            for route in self.routes:
                client.subscribe(route)

        self.client.on_connect = _on_connect
        print(f"Connecting to {hostname}:{port}")
        self.client.connect(hostname, port)

        self.logger = getLogger("Mqtt Client")

    def send_message(self, route: str, payload: dict):
        topic = f'{route}/{self.server}'

        self.logger.info(f'Sending message to {topic}')
        json_payload = json.dumps(payload)

        self._send_string(topic, json_payload)

    def send_message_serialized(self, message: Union[list[dict], str], route,
                                encodeb64: bool = False, valid_json=False, error=False):
        json_messages = Serializer().serialize(message, self.server, encodeb64, valid_json, is_error=error)

        print(f"Sending {json_messages}")

        for serialized_message in json_messages:
            self.send_message(route, serialized_message)

    def _send_string(self, topic: str, payload: str):
        single(topic, payload, hostname=self.hostname, port=self.port, protocol=MQTTv5)

    def register_route(self, route, callback):
        topic = f"{route}/{self.server}"
        print(f"Listening to topic: {topic}")
        self.client.message_callback_add(topic, callback)

    def listen(self):
        self.client.loop_forever()

    @staticmethod
    def wrapper(client: Client, _, message):
        parsed_message = json.loads(Serializer.decode_bytes(message.payload))
        return client, _, parsed_message

    def endpoint(self, route: str, force_json=False):
        def decorator(func):
            def wrapper(client: Client, _, message):
                print("Running wrapper!")
                parsed_message = json.loads(Serializer.decode_bytes(message.payload))
                return func(client, _, parsed_message)
            if force_json:
                self.register_route(route, wrapper)
            else:
                self.register_route(route, func)

            def inner(*args, **kwargs):
                pass

            return inner

        return decorator
