import time
import unittest
import threading

from random import randbytes
from src.MqttLibPy.client import MqttClient


class TestSerialize(unittest.TestCase):

    def test_serialize_bytes(self):
        downloads_folder = "/path/to/folder"
        file_path = "/path/to/file"
        broker_url = "mybroker.com"

        client = MqttClient(broker_url, 1883)

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
