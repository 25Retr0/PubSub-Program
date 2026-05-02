"""! @file pubsubserver.py
@author William Kelly (s4882158)
@ai Not Used
"""

""" Specification - pubsubserver
The pubsubclient program provides a command line interface for 
subscribing to particular topics (and therefore receiving messages that 
match those topics) and for publishing messages on particular topics.
The pubsubclient can only operate when connected to a pubsubserver and 
will need to be able to simultaneously handle messages from stdin and 
from the server.
"""

import json
import sys
import socket
from dataclasses import dataclass
from typing import Optional
from threading import Lock, Thread
from pubsubshared import *

### Constants ##################################################################
PROGRAM = "pubsubserver"
QUEUED_CONNS = 5            # NOTE: This can be increased if needed.

### Data Classes ###############################################################
@dataclass()
class Server:
    port: str
    server: str = "localhost"


@dataclass()
class ServErrCode:
    OK_CODE = 0
    DUP_CLIENT_CODE = 1
    UNKNOWN_CLIENT_CODE = 2


### Classes $$$$$###############################################################

class ServerProgramArgs:
    server_id: str = ""
    listenOnPort: Optional[str] = None
    servers: list[Server] = []
    error: bool = False

class PeerConnections:
    peers: list[Connection] = []

    @staticmethod
    def show_cannot_connect_peer_msg(argvalue: str) -> None:
        print_stderr(f"{PROGRAM}: can't connect to peer \"{argvalue}\"")

    @staticmethod
    def show_incompatible_peer_msg(argvalue: str) -> None:
        print_stderr(f"{PROGRAM}: Peer server not found at \"{argvalue}\"")

    @staticmethod
    def show_cannot_connect_self_msg() -> None:
        print_stderr(f"{PROGRAM}: Can't connect to self as peer")

    @staticmethod
    def show_already_connected_peer_msg(argvalue: str) -> None:
        print_stderr(
            f"{PROGRAM}: Already connected to peer server at \"{argvalue}\""
        )

    @staticmethod
    def show_dupe_server_peer_id_msg(argvalue: str) -> None:
        print_stderr(f"{PROGRAM}: Unable to connect to server \"{argvalue}\" " \
            "due to common server IDs")

    @staticmethod
    def show_peer_connected_msg(argvalue: str, serverid: str) -> None:
        print_stdout(
            f"{PROGRAM}: Connected to peer \"{serverid}\" at \"{argvalue}\"")

    @staticmethod
    def show_received_peer_connection_msg(serverid: str) -> None:
        print_stdout(f"{PROGRAM}: Connection received from peer \"{serverid}\"")


class ClientConnection():
    def __init__(self, sock: socket.socket, id: str):
        self.sock = sock
        self.id = id
        self.topics = []


class Commands:
    pass


class PubSubServer:

    def __init__(self, server_id: str):
        self._lock = Lock()
        self.id = server_id
        self.commands = Commands()
        self.messenger = MessageProtocol(is_server=True, id=self.id)
        self.clients: list[ClientConnection] = []


    def close_client_connection(self, client: ClientConnection) -> bool:
        with self._lock:
            for c in self.clients:
                if client == c:
                    c.sock.close()
                    return True

        return False

    def get_clients(self) -> list[ClientConnection]:
        with self._lock:
            return list(self.clients)

    def add_client(self, client: ClientConnection) -> None:
        with self._lock:
            self.clients.append(client)

    def is_duplicate_client_id(self, id: str) -> bool:
        for client in self.clients:
            if client.id == id:
                return True
        return False

    '''Messages for - Handling Standard Input'''


    '''Messages for - Handling messages from Clients and Peer Servers'''
    def show_client_connected_msg(self, client_id: str) -> None:
        print_stdout(f"{PROGRAM}: Client \"{client_id}\" has connected")

    def show_client_duplicate_msg(self, client_id: str) -> None:
        print_stdout(f"{PROGRAM}: Client ID \"{client_id}\" would be " \
            "duplicated - aborting connection")

    def show_unknown_client_msg(self) -> None:
        print_stderr(f"{PROGRAM}: Connection with unknown client aborted")

    def show_client_disconnect(self, client_id: str) -> None:
        print_stdout(f"{PROGRAM}: Client \"{client_id}\" has disconnected")

    def show_peer_shutdown_warning(self, server_id: str) -> None:
        print_stdout(f"{PROGRAM}: Peer server \"{server_id}\" shutting down")

    def show_peer_disconnected(self, server_id: str) -> None:
        print_stderr(f"{PROGRAM}: Peer server \"{server_id}\" disconnected")

### Error Handler ##############################################################
class Errors:
    USAGE_ERROR_CODE = 1
    BAD_SERVER_ID = 2
    UNABLE_TO_LISTEN = 3

    @staticmethod
    def usage_msg() -> str:
        return f"Usage: {PROGRAM} [--server [server]:port]... " \
                "[--listenon port] serverid"

    @staticmethod
    def bad_server_id_msg(server_id) -> str:
        return f"{PROGRAM}: bad server ID \"{server_id}\""

    @staticmethod
    def unable_to_listen_msg(port) -> str:
        return f"{PROGRAM}: can't listen on port \"{port}\""

    @staticmethod
    def unknown_error_msg() -> str:
            return f"{PROGRAM}: Unknown Error Detected"


### Functions ##################################################################

def show_error(error_code: int, **kwargs) -> None:
    """Given an error code, print the matching message."""
    match error_code:
        case Errors.USAGE_ERROR_CODE:
            print_stderr(Errors.usage_msg())
        case Errors.BAD_SERVER_ID:
            server_id = kwargs.get("id", "SERVER")
            print_stderr(Errors.bad_server_id_msg(server_id))
        case Errors.UNABLE_TO_LISTEN:
            port = kwargs.get("port", "PORT")
            print_stderr(Errors.unable_to_listen_msg(port))
        case _:
            print_stderr(Errors.unknown_error_msg())


def exit_program(error_code: int) -> None:
    """Exit from program with given error_code."""
    # print_stderr(f"\n--DEBUG--\nExited with code '{error_code}'")
    sys.exit(error_code)


def parse_arguments(arguments: list[str]) -> ServerProgramArgs:
    """ arguments: [--server [server]:port]... [--listenon listenport] server_id

    ... means can be specified multiple times
    option arguments can be in any order. But prior to serverid
    """
    program_args: ServerProgramArgs = ServerProgramArgs()

    if len(arguments) < 1:
        program_args.error = True
        return program_args
    
    # parse arguements up to last argument
    option_args: list[str] = arguments[:-1]
    last_arg: str = arguments[-1]

    seenListenOnOpt: bool = False

    i: int = 0
    while i < len(option_args):
        arg: str = option_args[i]

        if arg == "--server":
            i += 1  # consume next argument
            
            if i >= len(option_args):
                program_args.error = True
                return program_args

            serv_port = option_args[i].strip() # check this is allowed
            if not ":" in serv_port or serv_port.startswith("--"):
                program_args.error = True
                return program_args

            server, port = serv_port.split(":")
            if not port.strip():
                program_args.error = True
                return program_args

            s: Server = Server(port, server=(server or "localhost"))
            program_args.servers.append(s)

        elif arg == "--listenon" and not seenListenOnOpt:
            seenListenOnOpt = True
            i += 1   # consume next argument

            if i >= len(option_args):
                program_args.error = True
                return program_args

            port = option_args[i].strip() # check this is allowed (index)
            if not port or port.startswith("--"):
                program_args.error = True
                return program_args
            program_args.listenOnPort = port

        else: # Not allowed argument
            program_args.error = True
            return program_args

        i += 1

    # last argument is server_id
    program_args.server_id = last_arg.strip()
    if not program_args.server_id or program_args.server_id.startswith("--"):
        program_args.error = True

    return program_args


def attempt_listen(serv_port: str | None) -> Connection:
    """ Attempt to listen for incoming client and peer server connections.
    If --listenon option is given, then must attempt to listen on given port.
    Otherwise, listen on any available port."""

    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    connection: Connection = Connection(sock)

    try:
        if serv_port is None:
            port = 0
        elif serv_port.isdigit():
            port = int(serv_port)
        else:
            addr = socket.getaddrinfo(
                None, serv_port, socket.AF_INET, socket.SOCK_STREAM
            )
            connection.sock.bind(addr[0][4])
            connection.sock.listen(QUEUED_CONNS)
            connection.port = addr[0][4][1] # port for connection
            return connection

        connection.sock.bind(("0.0.0.0", port))
        connection.sock.listen(QUEUED_CONNS)
        connection.port = port

    except Exception:
        connection.error = True

    return connection


def connectToPeers(peer_servers: list[Server]) -> PeerConnections | None:
    """Attempt to connect to any peer servers given 'peer_servers'.
    Will attempt to do:
        1. Connect to the given server
        2. Validate it is communicating with a valid server
        3. Check peer is not itself
        4. Check if a connection has already been made with server
        5. Check if this connection results in duplicate server_ids
    """
    peerConnections: PeerConnections = PeerConnections()

    return None


# TODO: Move reading of a packet (recv(4) + msg) into a helper function
## Keep it a little cleaner
def handle_initial_conection(
        sock: socket.socket, server: PubSubServer) -> tuple[int, ClientConnection | None]:
    sock.settimeout(0.5)
    try:
        data = sock.recv(4)
        if len(data) < 4:
            server.show_unknown_client_msg()
            return (ServErrCode.UNKNOWN_CLIENT_CODE, None)

        success, msg_len = MessageProtocol.decode_len_msg(data)
        if not success:
            server.show_unknown_client_msg()
            return (ServErrCode.UNKNOWN_CLIENT_CODE, None)

        raw_msg = sock.recv(msg_len)
        decoded_msg = MessageProtocol.decode_msg(raw_msg)

        # Attempt msg decode to json
        try:
            msg_data = json.loads(decoded_msg)
            header = msg_data["header"]
            #client_type = msg_data["type_flag"] # used when doing peer connections
            client_id = msg_data["id"]

            if header != "1588":
                server.show_unknown_client_msg()
                return (ServErrCode.UNKNOWN_CLIENT_CODE, None)
            elif server.is_duplicate_client_id(client_id):
                server.show_client_duplicate_msg(client_id)
                return (ServErrCode.DUP_CLIENT_CODE, None)

            client: ClientConnection = ClientConnection(sock, client_id)
            server.add_client(client)
            return (ServErrCode.OK_CODE, client)
            
        except (json.JSONDecodeError, TypeError, KeyError):
            server.show_unknown_client_msg()
            return (ServErrCode.UNKNOWN_CLIENT_CODE, None)

    except socket.timeout:
        server.show_unknown_client_msg()
        return (ServErrCode.UNKNOWN_CLIENT_CODE, None)


def handle_client(sock: socket.socket, server: PubSubServer):
    try:
        err_code, client = handle_initial_conection(sock, server)
        if err_code == ServErrCode.UNKNOWN_CLIENT_CODE: 
            sock.close()
            return
        elif err_code == ServErrCode.DUP_CLIENT_CODE:
            # Notify client with flag
            raw_msg = server.messenger.gen_msg(server.messenger.DUP_ID_CODE)
            server.messenger.send_msg(sock, server.messenger.encode_msg(raw_msg))
            sock.close()
            return
        elif client != None:
            # Valid client connected - send response message
            server.show_client_connected_msg(client.id)
            raw_msg = server.messenger.gen_msg(server.messenger.OK_CODE)
            server.messenger.send_msg(sock, server.messenger.encode_msg(raw_msg))
            receive_from_client(sock, server)
        else: #client is None / Ivalid
            sock.close()
            return

    except Exception as e:
        print(e, sys.stderr)


def receive_from_client(sock: socket.socket, server: PubSubServer):
    while True:
        try:
            data = sock.recv(4)
            if len(data) < 4:
                return 

        except EOFError:
            print("poo")
            # Client abruptly disconnected?


def handle_server_commands():
    while True:
        try:
            admin_msg = input ("")
            if admin_msg == "/shutdown":
                break
        except EOFError:
            print("EOF EXCEPTION")
            break
    pass

def process_connections(port_connection: Connection, server: PubSubServer):
    """Continuously listen for new connections."""
    listening_socket = port_connection.sock
    while True:
        client_socket, _ = listening_socket.accept()
        client_thread = Thread(target=handle_client, 
                               args=(client_socket, server))
        client_thread.start()


def runServer(port_conn: Connection, server: PubSubServer):
    # Make server stdin processing thread    
    Thread(
        target=process_connections,
        args=(port_conn,server,), 
        daemon=True
    ).start()
    handle_server_commands()


### MAIN #######################################################################
def main():
    
    ## Command line argument parsing
    arguments: ServerProgramArgs = parse_arguments(sys.argv[1:])
    if arguments.error:
        show_error(Errors.USAGE_ERROR_CODE)
        exit_program(Errors.USAGE_ERROR_CODE)

    ## Server ID Checking
    if not is_valid_id(arguments.server_id):
        show_error(Errors.BAD_SERVER_ID, id=arguments.server_id)
        exit_program(Errors.BAD_SERVER_ID)

    ## Server Listening
    port_connection: Connection = attempt_listen(arguments.listenOnPort)
    if port_connection.error:
        show_error(Errors.UNABLE_TO_LISTEN,
                   port=(arguments.listenOnPort or port_connection.port))
        exit_program(Errors.UNABLE_TO_LISTEN)

    # TODO: Move to function VV
    print_stderr(f"{PROGRAM}: listening on port {port_connection.port}") 

    ## Connecting to Peer Servers
    # peer_connections: PeerConnections | None = connectToPeers(arguments.servers)

    ## Server Runtime Behaviour
    server: PubSubServer = PubSubServer(arguments.server_id)
    runServer(port_connection, server)

if __name__ == "__main__":
    main()
















