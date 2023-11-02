import unittest

from random import randbytes
from uuid import uuid4

from src.MqttLibPy.serializer import Serializer
from src.MqttLibPy.client import MqttClient

import threading


class TestSerialize(unittest.TestCase):

    def test_serialize_bytes(self):
        serializer = Serializer(str(uuid4()))
        serializer.MAX_MESSAGE_LENGTH = 5 * 1000 * 1000  # 5 MBs
        # message = self._get_n_mb(5)
        client = MqttClient("mybroker.com", 1883)

        @client.endpoint("test_bytes", is_file=True)
        def get_file(client, user_data, file):
            with open(f"/path/to/save/file/{file['data'][0]['filename']}", 'wb+') as f:
                f.write(file['bytes'])
                f.close()
            del file['bytes']
            print("Recibido:")
            print(file)
            file['bytes'] = ''

        threading.Thread(target=client.listen).start()

        client.send_file("test_bytes", "/path/to/my/file/myfile.mov")

    def _get_n_mb(self, mbs: int):
        return randbytes(mbs * 1000 * 1000)
