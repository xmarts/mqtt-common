import unittest

from random import randbytes
from uuid import uuid4

from src.MqttLibPy.serializer import Serializer
from src.MqttLibPy.client import MqttClient


class TestSerialize(unittest.TestCase):

    def test_serialize_bytes(self):
        serializer = Serializer(str(uuid4()))
        serializer.MAX_MESSAGE_LENGTH = 5 * 1000 * 1000  # 5 MBs
        # message = self._get_n_mb(5)
        client = MqttClient("example.com", 1883)

        client.send_file("test_bytes", "/some/path")

    def _get_n_mb(self, mbs: int):
        return randbytes(mbs * 1000 * 1000)
