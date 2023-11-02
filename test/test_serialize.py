import unittest

from random import randbytes
from uuid import uuid4

from src.MqttLibPy.serializer import Serializer
from src.MqttLibPy.client import MqttClient

import threading


class TestSerialize(unittest.TestCase):

    def test_serialize_bytes(self):
        client = MqttClient("mybroker.com", 1883)

        @client.endpoint("test_bytes", is_file=True)
        def get_file(client, user_data, file):
            with open(f"/path/to/save/file/{file['data'][0]['filename']}", 'wb+') as f:
                f.write(file['bytes'])
                f.close()

        threading.Thread(target=client.listen).start()

        client.send_file("test_bytes", "/path/to/my/file/myfile.mov")

    def _get_n_mb(self, mbs: int):
        return randbytes(mbs * 1000 * 1000)
