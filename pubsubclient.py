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

import json
import sys
import socket
from pubsubshared import *
from dataclasses import dataclass
import select
from typing import Optional
from threading import Lock, Thread

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


### Classes ####################################################################
class Commands:

    def __init__(self):
        # Strings
        self.subscribe = "/subscribe"
        self.unsubcribe = "/unsubscribe"
        self.topic = "/topic"
        self.sendfile = "/sendfile"
        self.listsubs = "/listsubs"
        self.publish = "/publish"
        self.quit = "/quit"

    def show_unknown_command_msg(self) -> None:
        print_stderr(f"{PROGRAM}: unknown command")

    def show_unknown_argumemts_msg(self, command: str) -> None:
        print_stderr(f"{PROGRAM}: unknown arguments(s) - usage: " \
            f"{self.get_usage_cmd(command)}")

    def show_no_def_topic_msg(self) -> None:
        print_stderr(f"{PROGRAM}: no default topic set")
    
    def show_invalid_message_msg(self) -> None:
        show_error(Errors.INVALID_MESSAGE_CODE)

    def show_invalid_topic_msg(self, topic: str) -> None:
        show_error(Errors.INVALID_TOPIC_CODE, topic=topic)


    def get_usage_cmd(self, command: str) -> str:
        match command:
            case self.subscribe: return f"{self.subscribe} topic [filter]"
            case self.topic: return f"{self.topic} topic"
            case _: return "Unknown usage"


class Client:
    def __init__(self, client_id: str, connection: Connection, topic: str | None):
        self.client_id = client_id
        self.commands = Commands()
        self.messenger = MessageProtocol(is_server=False, id=self.client_id)
        self.conn = connection

        self.default_topic = topic
        self._default_topic_lock = Lock()

        self.error_code = Errors.OK
        self._error_lock = Lock()

        self.subsriptions: list[Subscription] = []
        self._subscriptions_lock = Lock()


        self.client_quit = False
        self._client_quit_lock = Lock()

    def did_client_quit(self):
        with self._client_quit_lock:
            did_quit = self.client_quit
        return did_quit

    def set_client_quit(self, did: bool):
        with self._client_quit_lock:
            self.client_quit = did

    def get_error_code(self) -> int:
        with self._error_lock:
            err_code = self.error_code
        return err_code

    def set_error_code(self, err_code) -> None:
        with self._error_lock:
            self.error_code = err_code


    def get_default_topic(self) -> str | None:
        with self._default_topic_lock:
            topic = self.default_topic
        return topic

    def set_default_topic(self, topic: str) -> None:
        with self._default_topic_lock:
            self.default_topic = topic


    def publish(self, topic: str, message: str) -> int:
        # Check topic and message are valid
        if not is_valid_topic(topic):
            show_error(Errors.INVALID_TOPIC_CODE)
            return Errors.INVALID_TOPIC_CODE
        elif not is_valid_message(message):
            show_error(Errors.INVALID_MESSAGE_CODE)
            return Errors.INVALID_MESSAGE_CODE

        msg = self.messenger.gen_msg(self.messenger.PUBLISH_CODE, message)
        encoded_msg = self.messenger.encode_msg(msg)
        self.messenger.send_msg(self.conn.sock, encoded_msg)
        return Errors.OK


    def notify(self, msg_code: int) -> int:
        msg = self.messenger.gen_msg(msg_code)
        encoded_msg = self.messenger.encode_msg(msg)
        self.messenger.send_msg(self.conn.sock, encoded_msg)
        return Errors.OK


    def receive_from_server(self, conn: Connection) -> None:
        while self.get_error_code() == Errors.OK and not self.did_client_quit():
            try:
                data = conn.sock.recv(4)
                if not data:
                    if not self.did_client_quit():
                        self.set_error_code(Errors.ABRUPT_SERVER_CLOSE)
                    return

                success, msg_len = self.messenger.decode_len_msg(data)
                if not success:
                    print("protocol error")
                    continue

                raw_msg = conn.sock.recv(msg_len)
                decoded_msg = self.messenger.decode_msg(raw_msg)
                try:
                    msg_data = json.loads(decoded_msg)
                    # TODO: process msg

                except json.JSONDecodeError:
                    print("protocol error")
                    continue

            except (ConnectionResetError, BrokenPipeError):
                # set error code
                if not self.did_client_quit():
                    self.set_error_code(Errors.ABRUPT_SERVER_CLOSE)
                return

    def handle_user_input(self, client_input):
        if client_input == self.commands.quit:
            self.set_client_quit(True)
            self.notify(self.messenger.DISCON_CODE)
            self.set_error_code(Errors.OK)
            return
        elif client_input.startswith(self.commands.topic):
            topic_info = client_input.split(" ")
            if len(topic_info) != 2:
                self.commands.show_unknown_argumemts_msg(self.commands.topic)
            topic = topic_info[1].strip("\" ")
            if not is_valid_topic(topic):
                self.commands.show_invalid_topic_msg(topic)
            else:
                self.set_default_topic(topic)
        elif client_input.startswith(self.commands.publish):
            publish_info = client_input.split(" ")
            if len(publish_info) != 3:
                self.commands.show_unknown_argumemts_msg(self.commands.publish)
            topic, msg = publish_info[1].strip("\" "), publish_info[2].strip("\" ")
            self.publish(topic, msg)

        elif client_input.startswith("/"):
            # at this point would be an unknown command
            self.commands.show_unknown_command_msg()
        elif client_input == "":
            # Empty strings are ignored
            return
        else:
            topic = self.get_default_topic()
            if topic == None:
                self.commands.show_no_def_topic_msg()
            elif is_valid_message(client_input):
                self.publish(topic, client_input)
            else:
                self.commands.show_invalid_message_msg()

    def read_user_input(self) -> None:
        while self.get_error_code() == Errors.OK and not self.did_client_quit():
            try:
                # BUG: Ref AI usage here
                ready, _, _ = select.select([sys.stdin], [], [], 0.2)
                if ready:
                    # WARNING: potential bug for windows systems
                    client_input = sys.stdin.readline().rstrip().strip()
                    self.handle_user_input(client_input)
            except EOFError:
                print("EOF EXCEPTION")
                return

    def run(self, connection: Connection) -> int:
        Thread(
            target=self.receive_from_server,
            args=(connection,),
            daemon=True
        ).start()

        self.read_user_input()
        return self.get_error_code()


### Error Handler ##############################################################
class Errors:
    OK = 0
    USAGE_ERROR_CODE = 1
    BAD_CLIENT_ID_CODE = 4
    INVALID_TOPIC_CODE = 5
    INVALID_MESSAGE_CODE = 6
    UNABLE_TO_CONNECT_CODE = 7
    INVALID_SERVER_CODE = 8
    NON_UNIQUE_ID_CODE = 9
    ABRUPT_SERVER_CLOSE = 10

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
    def abrupt_server_close_msg() -> str:
        return f"{PROGRAM}: server disconnected - exiting"

    @staticmethod
    def unknown_error_msg() -> str:
        return f"{PROGRAM}: Unknown Error Detected"

### Helper Functions ###########################################################

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
        case Errors.ABRUPT_SERVER_CLOSE:
            print_stderr(Errors.abrupt_server_close_msg())
        case _:
            print_stderr(Errors.unknown_error_msg())


def exit_program(error_code: int) -> None:
    """Exit from program with given error_code."""
    # print_stderr(f"\n--DEBUG--\nExited with code '{error_code}'")
    sys.exit(error_code)


### Program Functions ##########################################################

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


### Networking Functions #######################################################
def attempt_connection(server: str, serv_port: str) -> Connection:
    """Attempt connection to server:port given from command line
    If unsuccessful return Connection object with error flag."""

    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    connection: Connection = Connection(sock)


    ## WARNING: If any connection errors happen, might be because getaddrinfo
    try:
        if serv_port.isdigit():
            port = int(serv_port)
        else:
            port = serv_port

        serverAddr = socket.getaddrinfo(
            server, port, socket.AF_INET, socket.SOCK_STREAM
        )

        connection.sock.connect(serverAddr[0][4])
        connection.port = serverAddr[0][4][1]   # get the port used

    except Exception as e:
        connection.error = True

    return connection


def handle_initial_connection(
        conn: Connection, arguments: ClientProgramArgs) -> tuple[int, Client]:
    """Server validity and client id checking.
    Waits up to 0.8 second for a server response. If no response, outputs
    error msg to stderr and attempt exit.
    Checks client id against server to ensure uniqueness
    """

    client: Client = Client(arguments.client_id, conn, arguments.topic)
    msg = client.messenger.gen_msg(MessageProtocol.CONN_CODE)
    msg = client.messenger.encode_msg(msg)
    client.messenger.send_msg(conn.sock, msg)

    conn.sock.settimeout(0.5)
    try:
        data = conn.sock.recv(4)
        if len(data) < 4:
            return Errors.INVALID_SERVER_CODE, client

        success, msg_len = MessageProtocol.decode_len_msg(data)
        if not success:
            return Errors.INVALID_SERVER_CODE, client

        raw_msg = conn.sock.recv(msg_len)
        decoded_msg = MessageProtocol.decode_msg(raw_msg)

        try:
            msg_data = json.loads(decoded_msg)
            header = msg_data["header"]
            code = msg_data["code"]

            if header != "1588":
                return Errors.INVALID_SERVER_CODE, client
            elif code == MessageProtocol.DUP_ID_CODE:
                return Errors.NON_UNIQUE_ID_CODE, client

            return Errors.OK, client

        except (json.JSONDecodeError, TypeError, KeyError):
            return Errors.INVALID_SERVER_CODE, client

    except socket.timeout:
        return Errors.INVALID_SERVER_CODE, client


def runClient(connection: Connection, arguments: ClientProgramArgs) -> int:
    """Handle runtime behaviour for client.
    Creates read thread that receives messages from server socket.
    Handles sending messages through server socket."""

    err, client = handle_initial_connection(connection, arguments)
    connection.sock.settimeout(None)
    if err != Errors.OK:
        show_error(err, server=arguments.server, port=connection.port, 
                   client_id=arguments.client_id)
        connection.sock.close()
        exit_program(err)
    elif arguments.topic != None and arguments.message != None:
        client.publish(arguments.topic, arguments.message) # won't throw error
    else:
        print_stdout(WELCOME_MSG)
        err = client.run(connection)
        if err != Errors.OK:
            show_error(err)
            connection.sock.close()
            exit_program(err)

    connection.sock.close()
    return err

### Main #######################################################################
def main():

    ## Command line argument parsing
    arguments: ClientProgramArgs = parse_arguments(sys.argv[1:])
    if arguments.error:
        show_error(Errors.USAGE_ERROR_CODE)
        exit_program(Errors.USAGE_ERROR_CODE)

    ## Client ID Checking
    if not is_valid_id(arguments.client_id):
        show_error(Errors.BAD_CLIENT_ID_CODE, client_id=arguments.client_id)
        exit_program(Errors.BAD_CLIENT_ID_CODE)

    ## Topic Checking
    if arguments.topic and not is_valid_topic(arguments.topic):
        show_error(Errors.INVALID_TOPIC_CODE, topic=arguments.topic)
        exit_program(Errors.INVALID_TOPIC_CODE)

    ## Message Checking
    if arguments.message and not is_valid_message(arguments.message):
        show_error(Errors.INVALID_MESSAGE_CODE)
        exit_program(Errors.INVALID_MESSAGE_CODE)

    ## Connection Checking
    connection: Connection = attempt_connection(arguments.server, arguments.port)
    connection.host = arguments.server
    if connection.error:
        server = arguments.server
        port = arguments.port
        show_error(Errors.UNABLE_TO_CONNECT_CODE, server=server, port=port)
        exit_program(Errors.UNABLE_TO_CONNECT_CODE)

    ## Client Runtime Behaviour
    err = runClient(connection, arguments)
    exit_program(err)

if __name__ == "__main__":
    main()
