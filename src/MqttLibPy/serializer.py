import json
import json as jsn
import base64
import re
import hashlib

from textwrap import wrap
from typing import Union


class Serializer:
    _MAX_MESSAGE_LENGTH_BYTES = 10 * 1000 * 1000  # 10MB
    _MAX_MESSAGE_LENGTH = 268435448
    _SEQ = 0
    # Almost max 32bit signed int value
    _MAX_SEQ = 2147483640

    def __init__(self, uuid: str):
        # Pasar esto a la db
        self.id = uuid

    def serialize(self, message: Union[str, list[dict], bytes], encodeb64: bool = False,
                  valid_json=False, is_error=False, token="", filename: str = "", metadata: dict = None) -> list[dict]:
        if encodeb64 and not valid_json and not isinstance(message, bytes):
            if isinstance(message, list):
                message = json.dumps(message)
            message = base64.b64encode(message.encode('utf-8'))
            if len(message) > self._MAX_MESSAGE_LENGTH:
                fragments = wrap(message.decode('utf-8'), self._MAX_MESSAGE_LENGTH)
            else:
                fragments = [message.decode('utf-8')]
            message_type = "text"
        elif isinstance(message, bytes):
            if len(message) > self.MAX_MESSAGE_LENGTH:
                raise Exception(f"Max payload size exceeded ({self.MAX_MESSAGE_LENGTH}B)")
            if metadata is None:
                metadata = {}
            metadata.update({"filename": filename or hashlib.md5(message).hexdigest()})
            fragments = [message]
            message_type = "file"
        elif isinstance(message, list) and valid_json:
            fragments = self._naive_knapsack(message)
            message_type = "json"
        elif not encodeb64 and isinstance(message, str):
            fragments = [message]
            message_type = "text"
        else:
            if not valid_json:
                raise RuntimeError("Type must be str for non json messages")
            else:
                raise RuntimeError(f"Incorrect data type for message: {type(message)}")

        current_seq = self.seq

        return [{
            "data": fragment if message_type != "file" else [metadata],
            "seq": current_seq,
            "current_fragment": n,
            "total_fragments": len(fragments),
            "last_fragment": n == len(fragments) - 1,
            "encoded": encodeb64,
            "is_valid_json": valid_json,
            "encoding": "b64 utf-8" if encodeb64 else False,
            "error": is_error,
            "token": token,
            "type": message_type,
            "md5_hash": "" if message_type != "file" else hashlib.md5(fragment).hexdigest(),
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
        return self._MAX_MESSAGE_LENGTH_BYTES

    @MAX_MESSAGE_LENGTH.setter
    def MAX_MESSAGE_LENGTH(self, val):
        self._MAX_MESSAGE_LENGTH_BYTES = val

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
            if i == len(objects) - 1 and len(current_message) != 0:
                messages.append(current_message)
        return messages

    def _naive_knapsack_bytes(self, objects: list[bytes]) -> list[list[bytes]]:
        return objects

    def filter_html_tags(self, text: str):
        pattern = r'<.*?>'
        filtered_string = re.sub(pattern, '', text)
        return filtered_string
