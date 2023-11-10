import json
import hashlib

from paho.mqtt.publish import single
from paho.mqtt.client import MQTTv5, Client, MQTTMessage
from cryptography.fernet import Fernet

from logging import getLogger
from typing import Union, Callable

from .serializer import Serializer


class MqttClient:

    def __init__(self, hostname: str, port: int, prefix: str = "", suffix: str = "", uuid="",
                 encryption_key: bytes = '', encryption_callback: Union[Callable[[str], str], None] = None):
        self.prefix = prefix
        self.suffix = suffix
        self.uuid = uuid

        self.hostname = hostname
        self.port = port
        self.encryption_key = encryption_key
        if encryption_callback:
            self.encryption_callback = encryption_callback

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
                                encodeb64: bool = False, valid_json=False, error=False, secure=False):
        """
        :param message: List of dicts or string to send.
        :param route: topic to send message to
        :param encodeb64: Not implemented
        :param valid_json: Indicates "message" is a valid parsable json (list[dict])
        :param error: Indicates this is an error message
        """
        json_messages = Serializer(self.uuid, self.encryption_key or self.encryption_callback(route)).serialize(message, encodeb64, valid_json, is_error=error, encrypt=secure)

        for serialized_message in json_messages:
            self.send_message(route, serialized_message)

    def _send_string(self, topic: str, payload: Union[str, bytes]):
        print(f"Sending string to {topic}")
        single(topic, payload, hostname=self.hostname, port=self.port, protocol=MQTTv5)

    def send_bytes(self, message: bytes, route: str, filename: str = '', metadata: dict = None, secure=False):
        if metadata is None:
            metadata = {}

        # Mandar la metadata por route y el archivo por route/
        serialized_message = (Serializer(self.uuid, self.encryption_key or self.encryption_callback(route))
                              .serialize(message, filename=filename, metadata=metadata, encrypt=secure))

        for msg in serialized_message:
            if secure and (self.encryption_key or self.encryption_callback):
                message = self._get_fernet(route).encrypt(message)
            elif secure:
                raise Exception("No encryption key was provided to the client in order to send an encrypted message")

            self.send_message(route, msg)
            self._send_string(f"{route}/file", message)

    def send_file(self, route: str, filepath: str, metadata: dict = None, secure=False):
        if metadata is None:
            metadata = {}
        with open(filepath, "rb") as f:
            file_name = f.name.split("/")[-1]
            file_bytes = f.read()
        print(f"Sending {file_name} of length {len(file_bytes)}")
        self.send_bytes(file_bytes, route, file_name, metadata, secure=secure)

    def register_route(self, route, callback):
        topic = f'{self.prefix}{route}{self.suffix}'
        self.routes.append(topic)
        print(f"Listening to topic: {topic}")
        self.client.message_callback_add(topic, callback)

    def listen(self):
        print(f"Connecting to {self.hostname}:{self.port}")
        self.client.connect(self.hostname, self.port)
        self.client.loop_forever()

    def endpoint(self, route: str, force_json=False, is_file=False, secure=False):
        """
        :param route: part of the route to listen to, the final route will be of the form {prefix}{route}{suffix}
        :param force_json: The message payload is in json format, and will be passed to the callback as a dict
        :param is_file: Indicates if the type of payload is a bytes object
        :param secure: Whether to decrypt payloads with the provided key or not
        :return:
        """
        def decorator(func):
            def wrapper_json(client: Client, _, message: MQTTMessage):
                parsed_message = json.loads(message.payload)
                if secure:
                    parsed_message['data'] = (Serializer(self.uuid,
                                                         self.encryption_key or self.encryption_callback(message.topic))
                                              .decrypt_str(parsed_message['data']))
                return func(client, _, parsed_message['data'])

            def wrapper_files(client: Client, user_data, message):
                if secure:
                    file_bytes = self._get_fernet(message.topic).decrypt(message.payload)
                else:
                    file_bytes = message.payload
                md5_hash = hashlib.md5(file_bytes).hexdigest()
                if md5_hash in self.files:
                    self.files[md5_hash]['bytes'] = file_bytes
                else:
                    # File arrived before the metadata, this shouldn't happen
                    self.logger.warning(f"File arrived before metadata. Hash: {md5_hash}")
                    self.files[md5_hash] = {}
                    self.files[md5_hash]['bytes'] = file_bytes
                    return

                func(client, user_data, self.files[md5_hash])

                # Cleanup
                del self.files[md5_hash]

            def wrapper_files_metadata(client: Client, user_data, message):
                parsed_message = json.loads(message.payload)
                if secure:
                    bytes_json = self._get_fernet(message.topic).decrypt(parsed_message['data'].encode('utf-8'))
                    string_json = bytes_json.decode('utf-8')
                    parsed_message['data'] = json.loads(string_json)
                parsed_message['data'] = json.loads(parsed_message['data'])

                if parsed_message['type'] != 'file':
                    self.logger.warning(f"A message of type {parsed_message['type']} was received on a file endpoint")
                    return

                if parsed_message['md5_hash'] in self.files:
                    # Metadata arrived late
                    self.files[parsed_message['md5_hash']]['md5_hash'] = parsed_message['md5_hash']
                    self.files[parsed_message['md5_hash']]['filename'] = parsed_message['data']['filename']
                    self.files[parsed_message['md5_hash']]['from'] = parsed_message['from']
                    self.files[parsed_message['md5_hash']]['data'] = parsed_message['data']
                    func(client, user_data, parsed_message['md5_hash'])
                    del self.files[parsed_message['md5_hash']]
                else:
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
                self.register_route(route, wrapper_files_metadata)
                self.register_route(f"{route}/file", wrapper_files)
            else:
                self.register_route(route, func)

            def inner(*args, **kwargs):
                pass

            return inner

        return decorator

    def _get_fernet(self, topic: str) -> Union[None, Fernet]:
        key = self.encryption_key or self.encryption_callback(topic)
        return Fernet(key)
