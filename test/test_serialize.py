import time
import unittest
import threading

from cryptography.fernet import Fernet
from random import randbytes
from src.MqttLibPy.client import MqttClient


class TestSerialize(unittest.TestCase):

    def test_secure_connection(self):
        return
        broker_url = "mybroker.com"

        key = Fernet.generate_key()
        Fernet(key)

        client = MqttClient(broker_url, 1883, encryption_key=key)

        @client.endpoint('test_route', force_json=True)
        def test_route(client, user_data, message):
            print("Message")
            print(message)

        threading.Thread(target=client.listen).start()
        time.sleep(2)

        obj = [{"message": "hola"}]
        client.send_message_serialized(obj, "test_route", valid_json=True)

    def test_serialize_file_bytes(self):
        downloads_folder = "/some/folder"
        file_path = "/some/file/path"
        broker_url = "mybroker.com"

        key = Fernet.generate_key()
        fernet = Fernet(key)

        client = MqttClient(broker_url, 1883, encryption_key=key)

        @client.endpoint("test_bytes", is_file=True)
        def get_file(client, user_data, file):
            with open(f"{downloads_folder}/{file['filename']}", 'wb+') as f:
                f.write(file['bytes'])
                f.close()

        threading.Thread(target=client.listen).start()

        client.send_file("test_bytes", file_path)
        time.sleep(20)
        client.send_file("test_bytes", file_path)

    def _get_n_mb(self, mbs: int):
        return randbytes(mbs * 1000 * 1000)
