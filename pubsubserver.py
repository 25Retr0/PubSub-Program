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

import sys
import socket
from dataclasses import dataclass
from typing import Optional
from threading import Lock, Thread, current_thread
from pubsubshared import *
from time import sleep ## NOTE: TESTING PURPOSES ONLY

### Constants ##################################################################
PROGRAM = "pubsubserver"
QUEUED_CONNS = 5            # NOTE: This can be increased if needed.


clients: list[socket.socket] = []                # WARNING: idk if this should just sit here

### Data Classes $##############################################################
@dataclass()
class Server:
    port: str
    server: str = "localhost"


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
    

### Error Handler ##############################################################
class Errors:
    USAGE_ERROR_CODE = 1
    BAD_SERVER_ID = 2
    UNABLE_TO_LISTEN = 3

    @staticmethod
    def usage_msg() -> str:
        return f"Usage: {PROGRAM} [--server [server]:port]..." \
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
    print_stderr(f"\n--DEBUG--\nExited with code '{error_code}'")
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
    if not program_args.server_id:
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
            connection.port = connection.sock.getsockname()[1]
            return connection

        connection.sock.bind(("", port))
        connection.sock.listen(QUEUED_CONNS)
        connection.port = connection.sock.getsockname()[1]

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


def handle_client(client_socket, client_address):
    # continuously receive data
    # based on message (recv) decide on
    #### broadcasting, or
    #### reply to client,
    #### or if peer, do more
    clients.append(client_socket)


def handle_server_commands():
    while True:
        admin_msg = input ("[ADMIN CMD]: ")
        if admin_msg == "/shutdown":
            break
        else:
            for c in clients:
                try:
                    c.send(admin_msg.encode())
                except:
                    clients.remove(c)

def process_connections(port_connection: Connection):
    """Continuously listen for new connections."""
    listening_socket = port_connection.sock
    while True:
        client_socket, client_addr = listening_socket.accept()
        client_thread = Thread(target=handle_client, 
                               args=(client_socket, client_addr))
        client_thread.start()


def runServer(port_conn: Connection):
    # Make server stdin processing thread    
    Thread(target=handle_server_commands, daemon=True).start()
    process_connections(port_conn)


### MAIN #######################################################################
def main():
    
    ## Command line argument parsing
    arguments: ServerProgramArgs = parse_arguments(sys.argv[1:])
    if arguments.error:
        show_error(Errors.USAGE_ERROR_CODE)
        exit_program(Errors.USAGE_ERROR_CODE)

    ## Server ID Checking
    if not isValidId(arguments.server_id):
        show_error(Errors.BAD_SERVER_ID)
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
    peer_connections: PeerConnections | None = connectToPeers(arguments.servers)

    ## Server Runtime Behaviour
    runServer(port_connection)

if __name__ == "__main__":
    main()
















