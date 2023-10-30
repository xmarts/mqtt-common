import json
import json as jsn
import base64
import re

from uuid import UUID, uuid4
from textwrap import wrap
from typing import Union


class Serializer:

    _MAX_MESSAGE_LENGTH = 40000
    _SEQ = 0
    # Almost max 32bit signed int value
    _MAX_SEQ = 2147483640

    def __init__(self):
        # Pasar esto a la db
        self.id = str(uuid4())

    def serialize(self, message: Union[str, list[dict]], encodeb64: bool = False,
                  valid_json=False, is_error=False, token="") -> list[dict]:
        if encodeb64 and not valid_json:
            if isinstance(message, list):
                message = json.dumps(message)
            message = base64.b64encode(message.encode('utf-8'))
            if len(message) > self._MAX_MESSAGE_LENGTH:
                fragments = wrap(message.decode('utf-8'), self._MAX_MESSAGE_LENGTH)
            else:
                fragments = [message.decode('utf-8')]
        elif isinstance(message, list) and valid_json:
            fragments = self._naive_knapsack(message)
        elif not encodeb64 and isinstance(message, str):
            fragments = [message]
        else:
            if not valid_json:
                raise RuntimeError("Type must be str for non json messages")
            else:
                raise RuntimeError(f"Incorrect data type for message: {type(message)}")

        current_seq = self.seq

        return [{
            "data": fragment,
            "seq": current_seq,
            "current_fragment": n,
            "total_fragments": len(fragments),
            "last_fragment": n == len(fragments)-1,
            "encoded": encodeb64,
            "is_valid_json": valid_json,
            "encoding": "b64 utf-8" if encodeb64 else False,
            "error": is_error,
            "token": token,
            "from": self.id
        } for n, fragment in enumerate(fragments)]

    def deserialize(self, message: str):
        """
        Doesnt support multi part messages yet
        @param message: raw str payload
        @return: Parsed, usable packet body
        """
        try:
            packet = json.loads(message)
            if packet["total_fragments"] > 1:
                raise NotImplementedError("Multi part messages are not implemented yet")
            if packet["encoded"]:
                str_message = packet["data"].decode('utf-8')
            else:
                str_message = packet["data"]

            if packet["is_valid_json"]:
                parsed_message = json.loads(str_message)
            else:
                parsed_message = str_message

            return parsed_message
        except Exception as e:
            print(f"An error has occurred during the parsing of a message {str(e)}")

    @property
    def seq(self):
        current = Serializer._SEQ
        Serializer._SEQ += 1
        return current

    def _as_str(self, obj):
        return jsn.dumps(obj, ensure_ascii=False)

    def _len(self, obj: Union[dict, list[dict]]):
        return len(self._as_str(obj))

    @property
    def MAX_MESSAGE_LENGTH(self):
        return self._MAX_MESSAGE_LENGTH

    @MAX_MESSAGE_LENGTH.setter
    def MAX_MESSAGE_LENGTH(self, val):
        self._MAX_MESSAGE_LENGTH = val + 5

    @staticmethod
    def decode_bytes(message):
        return message.decode("utf-8")

    def _naive_knapsack(self, objects: list[dict]) -> list[list[dict]]:
        """
        Takes a list of objects and returns a list of lists which sum of stringified
        objects are lower than self.MAX_MESSAGE_LENGTH.
        It's not optimized to return the best possible combinations, but it works.

        @postcondition all([sum(self._len(elem) for elem) < self.MAX_MESSAGE_LENGTH in message for message in output])
        """

        messages = []
        current_message = []
        for i, obj in enumerate(objects):
            if self._len([obj, *current_message]) < self.MAX_MESSAGE_LENGTH and obj:
                current_message.append(obj)
            elif self._len([obj, *current_message]) >= self.MAX_MESSAGE_LENGTH:
                messages.append(current_message)
                if self._len(obj) > self.MAX_MESSAGE_LENGTH:
                    raise Exception("Length of object is bigger than the maximum "
                                                   "allowed by this protocol. To solve this, enable fragmentation "
                                                   "with [{\"encode\": true}]")
                current_message = [obj]
            if i == len(objects)-1 and len(current_message) != 0:
                messages.append(current_message)
        return messages

    def filter_html_tags(self, text: str):
        pattern = r'<.*?>'
        filtered_string = re.sub(pattern, '', text)
        return filtered_string
