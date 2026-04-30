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
    port: str | int | None = None
    host: str = ""

### Functions ##################################################################
def print_stderr(message: str) -> None:
    """Helper method for printing a message to stderr."""
    print(message, file=sys.stderr)
    sys.stderr.flush()


def print_stdout(message: str) -> None:
    """Helper method for printing a message to stdout."""
    print(message, file=sys.stdout)
    sys.stdout.flush()


def isValidId(id: str) -> bool:
    """Given an id, returns True if:
        - must be between 2 and 32 characters (inclusive) in length.
        - contain only letters and/or digits
    """
    return ((2 <= len(id) <=32) and id.isalnum());


### Protocol + Protocol Functions ##############################################
"""
Message Structure: what is needed?

HEADER = 48821588
SERVER/CLIENT FLAG = 0 || 1
SERVER/CLIENT ID LEN = n
SERVER/CLIENT ID = "___"
MESSAGE_CODE = 0 || 1 || 2 ||3 ....etc  for handling publishing, requests, etc.
MESSAGE LEN = n
MESSAGE = "____"

USE JSON

# WARNING: potential for packets to be joined. need some sort of delimiter?
or send the size of the packet before the message to delimit it that way
"""

class MessageProtocol:
    CONN_CODE = 1
    CONN_RESP = 2

    def __init__(self, is_server: bool, id: str):
        self.client_serv_flag = 1 if is_server else 0
        self.id = id
        self.id_len = len(id)


    def gen_msg(self, msg_code: int, message: str = "") -> dict:
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
