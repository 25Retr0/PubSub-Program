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
    host: str = ""

class Subscription:
    def __init__(self, topic: str, op = "", arg = ""):
        self.topic = topic
        self.op = ""
        self.arg = ""

        self.valid_ops = ["<", "<=", ">", ">=", "==", "!="]

    def __eq__(self, other):
        return self.topic == other.topic

    def __str__(self):
        return f"{self.topic} {self.op or "-"} {self.arg or "-"}"

    def get_topic(self):
        return self.topic

    def get_op(self):
        return self.op

    def set_op(self, op: str) -> bool:
        if op in self.valid_ops:
            self.op = op
            return True
        return False

    def get_arg(self):
        return self.arg

    def set_arg(self, arg: str) -> bool:
        # TODO:
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
    # Check start character is a letter
    start_letter = topic[0]     # No index error -> non-empty from parsing
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

### Protocol + Protocol Functions ##############################################
class MessageProtocol:
    OK_CODE = 0 
    CONN_CODE = 1
    PUBLISH_CODE = 2
    DUP_ID_CODE = 3
    DISCON_CODE = 4
    SUBCRIBE_CODE = 5
    UNSUBCRIBE_CODE = 6

    def __init__(self, is_server: bool, id: str):
        self.client_serv_flag = 1 if is_server else 0
        self.id = id
        self.id_len = len(id)


    def gen_msg(self, 
                msg_code: int, 
                message: str | dict = ""
    ) -> dict:
        return {
            "header": "1588",
            "msg_id": 0, # TODO: Mutex for a global counter. Or attach to id
            "type_flag": 1 if self.client_serv_flag else 0,
            "id": self.id,
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
