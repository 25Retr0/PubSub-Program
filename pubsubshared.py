"""! @file pubsubclient.py
@author William Kelly (s4882158)
@ai Not Used
"""

import socket
import sys
import json
import struct
from dataclasses import dataclass
from typing import Optional 

### Data Classes ###############################################################
@dataclass
class Connection:
    sock: socket.socket
    error: bool = False
    port: str | int | bytes | None = None
    host: str | int = ""

class Subscription:
    def __init__(self, topic: str, op = "", arg = ""):
        self.topic = topic
        self.op = op
        self.arg = arg
        self.filter = ""

        self.valid_ops = ["<", "<=", ">", ">=", "==", "!="]

    def __eq__(self, other):
        return self.topic == other.topic and self.op == other.op and self.arg == other.arg

    def __str__(self):
        topic = self.topic
        if " " in topic:
            topic = f"\"{topic}\""

        if self.filter != "":
            if " " in self.filter:
                filter = f"\"{self.filter}\""
            else:
                filter = self.filter
            return f"/subscribe {topic} {filter}"
        else:
            return f"/subscribe {topic}"

    def get_topic(self):
        return self.topic

    def add_filter(self, filter) -> bool:
        self.filter = filter

        # checking filter, make a operator list that holds all tokens until a non operator is found
        operator_tokens = []
        op_val_split_idx = 0

        valid_ops = ["<", ">", "=", "!"]
        for i, c in enumerate(filter):
            if c in valid_ops:
                operator_tokens.append(c)
            else:
                op_val_split_idx = i
                break;

        op = "".join(operator_tokens)
        if op not in self.valid_ops:
            return False

        value = filter[op_val_split_idx:]
        try:
            value = float(value)
        except Exception:
            return False

        self.op = op
        self.arg = value
        return True

    def to_json_msg(self):
        return {
            "topic": self.topic,
            "op" : self.op,
            "arg": self.arg,
        }


### Functions ##################################################################
def print_stderr(message: str) -> None:
    """Helper method for printing a message to stderr."""
    print(message, file=sys.stderr)
    sys.stderr.flush()


def print_stdout(message: str) -> None:
    """Helper method for printing a message to stdout."""
    print(message, file=sys.stdout)
    sys.stdout.flush()


def is_valid_topic(topic: str) -> bool:
    """ A valid topic string consists of:
        - Start with a letter (upper or lower)
        - consist of letters, numbers, spaces, and/or '/' (forward slash)
        Returns True if conditions met, otherwise False
    """
    start_letter = topic[0]
    if not start_letter.isalpha():
        return False

    # Check remaining characters follow rules
    for char in topic:
        if char.isalnum() or char in [' ', '/']: continue
        return False
    return True


def is_valid_id(id: str) -> bool:
    """Given an id, returns True if:
        - must be between 2 and 32 characters (inclusive) in length.
        - contain only letters and/or digits
    """
    return ((2 <= len(id) <=32) and id.isalnum());


def is_valid_message(message: str) -> bool:
    """Returns True if the given message is printable. Otherwise False."""
    return message.isprintable()


def split_args(text: str, delimiter=" ", quote_char='"') -> list[str]:
    result = [] 
    current_tokens = []
    in_quotes = False

    for char in text:
        if char == quote_char:
            in_quotes = not in_quotes
        elif char == delimiter and not in_quotes:
            if current_tokens != []:
                result.append("".join(current_tokens).strip())
            current_tokens = []
        else:
            current_tokens.append(char)

    if current_tokens != []:
        result.append("".join(current_tokens).strip())

    return result

### Protocol + Protocol Functions ##############################################
class MessageProtocol:
    OK_CODE = 0 
    CONN_CODE = 1
    PUBLISH_CODE = 2
    DUP_ID_CODE = 3
    DISCON_CODE = 4
    SUBCRIBE_CODE = 5
    UNSUBCRIBE_CODE = 6
    SEND_FILE = 7

    PEER_CONN = 10
    PEER_DISCON = 11
    PEER_CLIENT_REQ = 12
    PEER_SELF_ID = 13
    PEER_NAME_CLASH = 14
    PEER_DIRECTLY_CONN = 15

    def __init__(self, is_server: bool, id: str, uid: str = ""):
        self.client_serv_flag = 1 if is_server else 0
        self.id = id
        self.id_len = len(id)
        self.uid = uid


    def gen_msg(self, 
                msg_code: int, 
                message: str | dict = ""
    ) -> dict:
        return {
            "header": "1588",
            "msg_id": 0, # TODO: Mutex for a global counter. Or attach to id
            "type_flag": 1 if self.client_serv_flag else 0,
            "id": self.id,
            "uid": self.uid,
            "code": msg_code,
            "message": message
        }

    def send_msg(self, sock: socket.socket, msg: bytes) -> None:
        header = struct.pack(">I", len(msg))
        sock.sendall(header + msg)

    @staticmethod
    def decode_len_msg(msg: bytes) -> tuple[bool, int]:
        length = struct.unpack(">I", msg)[0]

        try:
            length = int(length)
            return (True, length)
        except Exception: 
            return (False, 0)

    @staticmethod
    def decode_msg(msg: bytes) -> str:
        return msg.decode()

    @staticmethod
    def encode_msg(msg_json: dict) -> bytes:
        return json.dumps(msg_json).encode()
