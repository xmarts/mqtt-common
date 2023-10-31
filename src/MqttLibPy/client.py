import json

from paho.mqtt.publish import single
from paho.mqtt.client import MQTTv5, Client

from logging import getLogger
from typing import Union

from .serializer import Serializer


class MqttClient:

    def __init__(self, hostname: str, port: int, prefix: str = "", suffix: str = "", uuid=""):
        self.prefix = prefix
        self.suffix = suffix
        self.uuid = uuid

        self.hostname = hostname
        self.port = port

        self.routes = []

        self.client = Client("", userdata=None, protocol=MQTTv5)

        def _on_connect(client: Client, _, __, ___, ____):
            for route in self.routes:
                client.subscribe(route)

        self.client.on_connect = _on_connect

        self.logger = getLogger("Mqtt Client")

    def send_message(self, topic: str, payload: dict):
        print(f'Sending message to {topic}')
        json_payload = json.dumps(payload)

        self._send_string(topic, json_payload)

    def send_message_serialized(self, message: Union[list[dict], str], route,
                                encodeb64: bool = False, valid_json=False, error=False):
        """
        :param message: List of dicts or string to send.
        :param route: topic to send message to
        :param encodeb64: Not implemented
        :param valid_json: Indicates "message" is a valid parsable json (list[dict])
        :param error: Indicates this is an error message
        """
        json_messages = Serializer().serialize(message, encodeb64, valid_json, is_error=error)

        for serialized_message in json_messages:
            self.send_message(route, serialized_message)

    def _send_string(self, topic: str, payload: str):
        single(topic, payload, hostname=self.hostname, port=self.port, protocol=MQTTv5)

    def register_route(self, route, callback):
        topic = f'{self.prefix}{route}{self.suffix}'
        self.routes.append(topic)
        print(f"Listening to topic: {topic}")
        self.client.message_callback_add(topic, callback)

    def listen(self):
        print(f"Connecting to {self.hostname}:{self.port}")
        self.client.connect(self.hostname, self.port)
        self.client.loop_forever()

    @staticmethod
    def wrapper(client: Client, _, message):
        parsed_message = json.loads(Serializer.decode_bytes(message.payload))
        return client, _, parsed_message

    def endpoint(self, route: str, force_json=False):
        """
        :param route: part of the route to listen to, the final route will be of the form {prefix}{route}{suffix}
        :param force_json: The message payload is in json format, and will be passed to the callback as a dict
        :return:
        """
        def decorator(func):

            def wrapper(client: Client, _, message):
                parsed_message = json.loads(Serializer.decode_bytes(message.payload))
                return func(client, _, parsed_message['data'])

            if force_json:
                self.register_route(route, wrapper)
            else:
                self.register_route(route, func)

            def inner(*args, **kwargs):
                pass

            return inner

        return decorator
