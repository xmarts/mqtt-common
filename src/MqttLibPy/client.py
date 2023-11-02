import json
import hashlib

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
        self.files = {}

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
        json_messages = Serializer(self.uuid).serialize(message, encodeb64, valid_json, is_error=error)

        for serialized_message in json_messages:
            self.send_message(route, serialized_message)

    def _send_string(self, topic: str, payload: Union[str, bytes]):
        print(f"Sending string to {topic}")
        single(topic, payload, hostname=self.hostname, port=self.port, protocol=MQTTv5)

    def send_bytes(self, message: bytes, route: str, filename: str = ''):
        # Mandar la metadata por route y el archivo por route/
        serialized_message = Serializer(self.uuid).serialize(message, filename=filename)

        for msg in serialized_message:
            self.send_message(route, msg)
            self._send_string(f"{route}/file", message)

    def send_file(self, route: str, filepath: str):
        with open(filepath, "rb") as f:
            file_name = f.name.split("/")[-1]
            file_bytes = f.read()
        print(f"Sending {file_name} of length {len(file_bytes)}")
        self.send_bytes(file_bytes, route, file_name)

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

    def endpoint(self, route: str, force_json=False, is_file=False, file_path=''):
        """
        :param route: part of the route to listen to, the final route will be of the form {prefix}{route}{suffix}
        :param force_json: The message payload is in json format, and will be passed to the callback as a dict
        :param is_file: Indicates if the type of payload is a bytes object
        :param file_path: Path to save files if is_file is True
        :return:
        """
        def decorator(func):

            def wrapper_json(client: Client, _, message):
                parsed_message = json.loads(Serializer.decode_bytes(message.payload))
                return func(client, _, parsed_message['data'])

            def wrapper_files(client: Client, user_data, message):
                file_bytes = message.payload
                md5_hash = hashlib.md5(file_bytes).hexdigest()
                if md5_hash in self.files:
                    self.files[md5_hash]['bytes'] = file_bytes
                else:
                    # File arrived before the metadata, this shouldn't happen
                    self.logger.warning(f"File arrived before metadata. Hash: {md5_hash}")

                func(client, user_data, self.files[md5_hash])

                # Cleanup
                del self.files[md5_hash]['bytes']

            def wrapper_files_metadata(client: Client, user_data, message):
                parsed_message = json.loads(Serializer.decode_bytes(message.payload))
                if parsed_message['type'] is not 'file':
                    self.logger.warning(f"A message of type {parsed_message['type']} was received on a file endpoint")
                    return

                self.files[parsed_message['md5_hash']] = {
                                                            'md5_hash': parsed_message['md5_hash'],
                                                            'filename': parsed_message['data']['filename'],
                                                            'from': parsed_message['from'],
                                                            'bytes': b'',
                                                            'data': parsed_message['data']
                                                          }

            if force_json:
                self.register_route(route, wrapper_json)
            elif is_file:
                if not file_path:
                    raise Exception("File endpoints require a file_path to be provided")
                self.register_route(route, wrapper_files_metadata)
                self.register_route(f"{route}/file", wrapper_files)
            else:
                self.register_route(route, func)

            def inner(*args, **kwargs):
                pass

            return inner

        return decorator
