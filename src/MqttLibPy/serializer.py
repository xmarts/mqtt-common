import json
import json as jsn
import base64
import re
import hashlib

from cryptography.fernet import Fernet
from textwrap import wrap
from typing import Union, List


class Serializer:
    _MAX_MESSAGE_LENGTH_BYTES = 10 * 1000 * 1000  # 10MB
    _MAX_MESSAGE_LENGTH = 268435448

    def __init__(self, uuid: str, key: bytes = None):
        # Pasar esto a la db
        self.id = uuid
        if key is not None:
            self.fernet = Fernet(key)

    def serialize(self, message: Union[str, List[dict], bytes], encodeb64: bool = False,
                  valid_json=False, is_error=False, filename: str = "",
                  metadata: dict = None, encrypt: bool = False) -> List[dict]:
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
            fragments = [json.dumps(metadata)]
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

        if encrypt and (message_type == 'file' or valid_json):
            # Si es file es siempre un solo fragmento
            fragments = list(map(lambda f: self.encrypt_json(f), fragments))
        elif encrypt and not valid_json:
            fragments = list(map(lambda f: self.encrypt_string(f), fragments))

        return [{
            "data": fragment,
            "current_fragment": n,
            "total_fragments": len(fragments),
            "last_fragment": n == len(fragments) - 1,
            "is_valid_json": valid_json,
            "error": is_error,
            "type": message_type,
            "encrypted": encrypt,
            "md5_hash": "" if message_type != "file" else hashlib.md5(message).hexdigest(),
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

    def _as_str(self, obj):
        return jsn.dumps(obj, ensure_ascii=False)

    def _len(self, obj: Union[dict, List[dict]]):
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

    def encrypt_json(self, message: dict) -> str:
        return self.encrypt_string(json.dumps(message))

    def encrypt_string(self, message: str) -> str:
        encoded_str = message.encode('utf-8')
        encrypted_bytes = self.fernet.encrypt(encoded_str)
        return encrypted_bytes.decode('utf-8')

    def decrypt_str(self, message: str):
        return self.fernet.decrypt(message.encode('utf-8')).decode('utf-8')

    def _naive_knapsack(self, objects: List[dict]) -> List[List[dict]]:
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

    def _naive_knapsack_bytes(self, objects: List[bytes]) -> List[List[bytes]]:
        return objects

    def filter_html_tags(self, text: str):
        pattern = r'<.*?>'
        filtered_string = re.sub(pattern, '', text)
        return filtered_string
