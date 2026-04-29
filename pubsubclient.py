"""! @file pubsubclient.py
@author William Kelly (s4882158)
@ai Not Used
"""

""" Specification - pubsubclient
The pubsubclient program provides a command line interface for 
subscribing to particular topics (and therefore receiving messages that 
match those topics) and for publishing messages on particular topics. 
The pubsubclient can only operate when connected to a pubsubserver and 
will need to be able to simultaneously handle messages from stdin and from 
the server.
"""

import sys
import socket
from pubsubshared import *
from dataclasses import dataclass
from typing import Optional
from threading import Lock, Thread, current_thread

### Constants ##################################################################
PROGRAM = "pubsubclient"
WELCOME_MSG = f"Welcome to {PROGRAM}!"

### Data Classes ###############################################################
@dataclass()
class ClientProgramArgs:
    port: str = ""
    client_id: str = ""
    topic: Optional[str] = None
    server: str = "localhost"
    message: Optional[str] = None
    error: bool = False


class Commands:
    pass


### Error Handler ##############################################################
class Errors:
    USAGE_ERROR_CODE = 1
    BAD_CLIENT_ID_CODE = 4
    INVALID_TOPIC_CODE = 5
    INVALID_MESSAGE_CODE = 6
    UNABLE_TO_CONNECT_CODE = 7
    INVALID_SERVER_CODE = 8
    NON_UNIQUE_ID_CODE = 9

    @staticmethod
    def usage_msg()-> str:
        return f"Usage: {PROGRAM} [--topic topic] [server]:port " \
                "clientid [message]"

    @staticmethod
    def bad_client_id_msg(client_id: str) -> str:
        return f"{PROGRAM}: bad client ID \"{client_id}\""

    @staticmethod
    def invalid_topic_msg(topic: str) -> str:
        return f"{PROGRAM}: invalid topic string \"{topic}\""

    @staticmethod
    def invalid_message_msg() -> str:
        return f"{PROGRAM}: messages must only contain printable characters"

    @staticmethod
    def unable_to_connect_msg(server: str, port: str) -> str:
        return f"{PROGRAM}: unable to connect to \"{server}:{port}\""

    @staticmethod
    def invalid_server_msg(server: str, port: str) -> str:
        return f"{PROGRAM}: server at \"{server}:{port}\" is not a valid server"

    @staticmethod
    def non_unique_id_msg(client_id: str) -> str:
        return f"{PROGRAM}: client ID \"{client_id}\" is not unique"

    @staticmethod
    def unknown_error_msg() -> str:
        return f"{PROGRAM}: Unknown Error Detected"

### Functions ##################################################################


def show_error(error_code: int, **kwargs) -> None:
    """Given an error code, print the matching message"""
    match error_code:
        case Errors.USAGE_ERROR_CODE:
            print_stderr(Errors.usage_msg())
        case Errors.BAD_CLIENT_ID_CODE:
            msg = kwargs.get("client_id", "CLIENTID")
            print_stderr(Errors.bad_client_id_msg(msg))
        case Errors.INVALID_TOPIC_CODE:
            msg = kwargs.get("topic", "TOPIC")
            print_stderr(Errors.invalid_topic_msg(msg))
        case Errors.INVALID_MESSAGE_CODE:
            print_stderr(Errors.invalid_message_msg())
        case Errors.UNABLE_TO_CONNECT_CODE:
            server = kwargs.get("server", "SERVER")
            port = kwargs.get("port", "PORT")
            print_stderr(Errors.unable_to_connect_msg(server, port))
        case Errors.INVALID_SERVER_CODE:
            server = kwargs.get("server", "SERVER")
            port = kwargs.get("port", "PORT")
            print_stderr(Errors.invalid_server_msg(server, port))
        case Errors.NON_UNIQUE_ID_CODE:
            msg = kwargs.get("client_id", "CLIENTID")
            print_stderr(Errors.non_unique_id_msg(msg))
        case _:
            print_stderr(Errors.unknown_error_msg())


def exit_program(error_code: int) -> None:
    """Exit from program with given error_code."""
    print_stderr(f"\n--DEBUG--\nExited with code '{error_code}'")
    sys.exit(error_code)


def parse_arguments(arguments: list[str]) -> ClientProgramArgs:
    """ arugments: [--topic topic] [server]:port clientid [message]"""
    program_args: ClientProgramArgs = ClientProgramArgs()

    arg: int = 0
    
    # if --topic exists -- MIN_ARGS + 2 is needed.
    args_len = len(arguments)
    if args_len == 0:
        program_args.error = True
        return program_args
    elif arguments[arg] == "--topic" and args_len >= 4:
        arg += 1
        topic_arg = arguments[arg]

        if topic_arg.strip() == '':
            program_args.error = True
            return program_args

        program_args.topic = topic_arg
        arg += 1 # move to next argument
    elif ((arguments[arg] == "--topic" and args_len < 4)
        or arguments[arg].startswith("--")):
        program_args.error = True
        return program_args

    # Now check if length of args is within MAX_ARGS (5)
    if (args_len > 5):
        program_args.error = True
        return program_args

    if args_len - arg < 2: # not enough arguments left
        program_args.error = True
        return program_args

    ## SERVER:PORT
    # First check string contains ":"

    if not (":" in arguments[arg]):
        program_args.error = True
        return program_args

    server, port = arguments[arg].split(":")
    port = port.strip()
    server = server.strip()
    if (not port or server.startswith("--")): 
        program_args.error = True
        return program_args

    if server != '':    # Check it was given as it is optional
        program_args.server = server

    program_args.port = port

    ## CLIENT ID

    arg += 1
    client_id = arguments[arg].strip()
    if not client_id:
        program_args.error = True
        return program_args
    program_args.client_id = client_id

    ## MESSAGE

    arg += 1

    if (args_len == 5 and program_args.topic != None):
        # allow message argument
        message = arguments[arg].strip()

        if message == '': # is empty
            program_args.error = True

        program_args.message = message

    elif (args_len >= 3 and program_args.topic == None):
        program_args.error = True

    return program_args


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


def isValidMessage(message: str) -> bool:
    """Returns True if the given message is printable. Otherwise False."""
    return message.isprintable()


def attempt_connection(server: str, serv_port: str) -> Connection:
    """Attempt connection to server:port given from command line arguments.sock. bind(('', port))
except Exception
    If unsuccessful return Connection object with error flag."""

    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    connection: Connection = Connection(sock)


    ## NOTE: If any connection errors happen, might be because getaddrinfo
    try:
        if serv_port.isdigit():
            port = int(serv_port)
        else:
            port = serv_port

        serverAddr = socket.getaddrinfo(
            server, port, socket.AF_INET, socket.SOCK_STREAM
        )

        connection.sock.connect(serverAddr[0][4])
    except Exception:
        connection.error = True

    return connection


def receive_handler(sock: socket.socket) -> None:
    """Receive messages from the server and process."""

    while True:
        try:
            message = sock.recv(1024).decode()
            if message:
                print_stdout(f"\r[Server] {message}\n")
        except:
            print_stderr("Connection Lost\n")
            break


def client_message_handler(sock: socket.socket) -> None:
    """Read lines from stdin and process."""
    print("Connected")
    while True:
        msg = input("Me: ")
        if msg.lower() == "exit":
            break

        if msg:
            sock.send(msg.encode())


def runClient(connection: Connection) -> None:
    """Handle runtime behaviour for client.
    Creates read thread that receives messages from server socket.
    Handles sending messages through server socket."""

    print_stdout(WELCOME_MSG)
    sock = connection.sock

    # Create receiving thread for handling messages from connection socket
    # WARNING: Please put a reference to this. Docs page probably
    Thread(target=receive_handler, args=(sock,), daemon=True).start()

    # Now continuously read input from stdin
    client_message_handler(sock)

    sock.close()


### Main #######################################################################
def main():

    ## Command line argument parsing
    arguments: ClientProgramArgs = parse_arguments(sys.argv[1:])
    if arguments.error:
        show_error(Errors.USAGE_ERROR_CODE)
        exit_program(Errors.USAGE_ERROR_CODE)

    ## Client ID Checking
    if not isValidId(arguments.client_id):
        show_error(Errors.BAD_CLIENT_ID_CODE, client_id=arguments.client_id)
        exit_program(Errors.BAD_CLIENT_ID_CODE)

    ## Topic Checking
    if arguments.topic and not is_valid_topic(arguments.topic):
        show_error(Errors.INVALID_TOPIC_CODE, topic=arguments.topic)
        exit_program(Errors.INVALID_TOPIC_CODE)

    ## Message Checking
    if arguments.message and not isValidMessage(arguments.message):
        show_error(Errors.INVALID_MESSAGE_CODE)
        exit_program(Errors.INVALID_MESSAGE_CODE)

    ## Connection Checking
    connection: Connection = attempt_connection(arguments.server, arguments.port)
    if connection.error:
        server = arguments.server
        port = arguments.port
        show_error(Errors.UNABLE_TO_CONNECT_CODE, server=server, port=port)
        exit_program(Errors.UNABLE_TO_CONNECT_CODE)

    ## Client Runtime Behaviour
    runClient(connection)


    ## Server Validity Checking
    #### TODO: MAKE PROTOCOL HERE
    # is_compatible = isCompatibleServer(connection.sock)
    # if not is_compatible:
    #     show_error(Errors.INVALID_SERVER_CODE)
    #     exit_program(Errors.INVALID_SERVER_CODE)

    ## Client Uniqueness Checking

if __name__ == "__main__":
    main()
