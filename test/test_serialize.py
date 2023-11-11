import time
import unittest
import threading

from cryptography.fernet import Fernet
from random import randbytes
from src.MqttLibPy.client import MqttClient


class TestSerialize(unittest.TestCase):

    BROKER_URL = "mybroker.com"
    DOWNLOADS_FOLDER = "/my/downloads/folder"
    FILE_PATH = "/my/file/path"

    def test_key_callback(self):
        return
        key = Fernet.generate_key()
        Fernet(key)

        db = {
            'mycompany': key
        }

        def callback_key(topic: str):
            print(topic)
            company_name = topic.split('/')[1]
            return db[company_name]

        client = MqttClient(TestSerialize.BROKER_URL, 1883, encryption_callback=callback_key)

        @client.endpoint('company/+/test_route', force_json=True, secure=True)
        def test_route(client, user_data, message):
            print("Message")
            print(message)

        threading.Thread(target=client.listen).start()
        time.sleep(2)

        sender_client = MqttClient(TestSerialize.BROKER_URL, 1883, encryption_key=key)

        obj = [{"message": "hola"}]
        sender_client.send_message_serialized(obj, "company/mycompany/test_route", valid_json=True, secure=True)

    def test_secure_connection(self):
        return
        key = Fernet.generate_key()
        Fernet(key)

        client = MqttClient(TestSerialize.BROKER_URL, 1883, encryption_key=key)

        @client.endpoint('test_route', force_json=True)
        def test_route(client, user_data, message):
            print("Message")
            print(message)

        threading.Thread(target=client.listen).start()
        time.sleep(2)

        obj = [{"message": "hola"}]
        client.send_message_serialized(obj, "test_route", valid_json=True)

    def test_serialize_file_bytes(self):
        return
        key = Fernet.generate_key()
        fernet = Fernet(key)

        db = {
            'mycompany': key
        }

        def callback_key(topic: str):
            company_name = topic.split('/')[1]
            return db[company_name]

        client = MqttClient(self.BROKER_URL, 1883, encryption_callback=callback_key)

        @client.endpoint("company/+/test_bytes", is_file=True, secure=True)
        def get_file(client, user_data, file):
            with open(f"{self.DOWNLOADS_FOLDER}/{file['filename']}", 'wb+') as f:
                f.write(file['bytes'])
                f.close()

        threading.Thread(target=client.listen).start()

        sender_client = MqttClient(self.BROKER_URL, 1883, encryption_key=key)

        sender_client.send_file("company/mycompany/test_bytes", self.FILE_PATH, secure=True)
        time.sleep(20)
        sender_client.send_file("company/mycompany/test_bytes", self.FILE_PATH, secure=True)

    def _get_n_mb(self, mbs: int):
        return randbytes(mbs * 1000 * 1000)
