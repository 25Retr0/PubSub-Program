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
import select
from uuid import uuid4
from dataclasses import dataclass
from typing import Optional
from threading import Lock, Thread
from pubsubclient import Client
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
    CANNOT_CONNECT_PEER = 3
    INCOMPATIBLE_PEER = 4
    CANNOT_CONNECT_SELF = 5
    DIRECTLY_CONNECTED_PEER = 6
    DUP_SERVER_ID = 7
    ALREADY_CONNECTED = 8


### Classes ####################################################################

class ServerProgramArgs:
    def __init__(self):
        self.server_id: str = ""
        self.listenOnPort: Optional[str] = None
        self.servers: list[Server] = []
        self.error: bool = False


class ClientConnection():
    def __init__(self, sock: socket.socket, id: str):
        self.sock = sock
        self.id = id
        self.subscriptions: list[Subscription] = []

    def __eq__(self, other) -> bool:
        if isinstance(other, ClientConnection):
            return self.id == other.id
        return False

    def get_subscriptions(self):
        topics = []
        for sub in self.subscriptions:
            topics.append(sub.topic)
        return topics 

    def matches_subscription(self, topic, msg):
        # If the given topic matches and no filter is present then just send
        for sub in self.subscriptions:
            if topic == sub.topic and sub.op == "" and sub.arg == "":
                return True
            elif topic == sub.topic:
                # check filter
                try:
                    # (a) message is a numerical value (float)
                    msg_value = float(msg["msg"])
                    # and (b) value of message meets condition
                    match sub.op:
                        case "<":
                            if msg_value < float(sub.arg): return True
                        case "<=":
                            if msg_value <= float(sub.arg): return True
                        case ">":
                            if msg_value > float(sub.arg): return True
                        case ">=":
                            if msg_value >= float(sub.arg): return True
                        case "==":
                            if msg_value == float(sub.arg): return True
                        case "!=":
                            if msg_value != float(sub.arg): return True
                        case _:
                            print("unknown op")
                            continue;
                except Exception as e:
                    continue

        return False



class PeerConnections:
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


class PubSubServer:
    def __init__(self, server_id: str, ip, port):
        self.id = server_id
        self.ip = ip
        self.port = port
        self.uid = str(uuid4())
        self.messenger = MessageProtocol(is_server=True, id=self.id, uid=self.uid)
        self.clients: list[ClientConnection] = []
        self._clients_lock = Lock()


        self.peers = []
        # self.peers = [
        # { "id": "Server_A", "socket": <socket_obj>, "address": "127.0.0.1:5000" },
        # ]
        self._peers_lock = Lock()

        self.federation_map = {}
        # self.federation_map = {
        #     "Server_A": "uuid-111",
        # }
        self._federation_map_lock = Lock()

    # --- Peer Management --- #
    
    def attempt_connection_to_peer(self, server: str, serv_port: str) -> tuple[int, Connection]:
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        connection: Connection = Connection(sock)

        try:
            if serv_port.isdigit():
                port = int(serv_port)
            else:
                port = serv_port

            serverAddr = socket.getaddrinfo(
                server, port, socket.AF_INET, socket.SOCK_STREAM
            )

            connection.sock.connect(serverAddr[0][4])
            connection.port = serverAddr[0][4][1]

        except Exception as e:
            connection.error = True
            sock.close()
            return (ServErrCode.CANNOT_CONNECT_PEER, connection)

        return (ServErrCode.OK_CODE, connection)


    def handshake_with_peer(self, conn: Connection):
        msg = self.messenger.gen_msg(self.messenger.PEER_CONN)
        self.messenger.send_msg(conn.sock, self.messenger.encode_msg(msg))
        
        conn.sock.settimeout(0.5)
        try:
            data = conn.sock.recv(4)
            print(data.decode)
        except socket.timeout:
            print("timeout")

    def recv_handshake_req_from_peer(self, sock, msg_data) -> tuple[int, None | dict]:
        return 0, None

    # def init_peer_connection(self, conn: Connection):
    #     # A server asking for a peer connection
    #     # Handle like the server is a client
    #
    #     # Send my data to the peer im connecting to
    #     known_peers = []
    #     with self._federation_peers_lock:
    #         for peer in self.federtion_peers:
    #             known_peers.append(peer.identifier_obj)
    #
    #     msg = self.messenger.gen_msg(self.messenger.CONN_CODE, {"known_peers": known_peers})
    #     msg = self.messenger.encode_msg(msg)
    #     self.messenger.send_msg(conn.sock, msg)
    #
    #     conn.sock.settimeout(0.5)
    #     # Check im getting data back, whether there's a naming conflict etc
    #     try:
    #         data = conn.sock.recv(4)
    #         if len(data) < 4:
    #             return ServErrCode.CANNOT_CONNECT_PEER, None
    #         success, msg_len = self.messenger.decode_len_msg(data)
    #         if not success:
    #             return ServErrCode.CANNOT_CONNECT_PEER, None
    #
    #         raw_msg = conn.sock.recv(msg_len)
    #         decoded_msg = self.messenger.decode_msg(raw_msg)
    #         try:
    #             msg_data = json.loads(decoded_msg)
    #             id = msg_data["id"]
    #
    #             if id == self.id:
    #                 return ServErrCode.CANNOT_CONNECT_SELF, None
    #
    #             with self._peers_lock:
    #                 if any(p.id == self.id for p in self.peers):
    #                     return ServErrCode.DIRECTLY_CONNECTED_PEER, None
    #
    #             # check federation
    #             with self._federation_peers_lock:
    #                 for peer in self.federtion_peers:
    #                     print(peer)
    #
    #             return ServErrCode.OK_CODE, Peer(conn.sock, id)
    #
    #         except:
    #             return ServErrCode.CANNOT_CONNECT_PEER, None
    #
    #     except socket.timeout:
    #         return ServErrCode.CANNOT_CONNECT_PEER, None
    #
    #
    # def recv_init_peer_connection(self, sock: socket.socket):
    #     # A server receiving a peer connection
    #     pass
    #
    #
    #
    # def connect_to_peers(self, potential_peers: list[Server]):
    #     """Attempt to connect to any peer servers given 'peer_servers'.
    #     Will attempt to do:
    #         1. Connect to the given server
    #         2. Validate it is communicating with a valid server
    #         3. Check peer is not itself
    #         4. Check if a connection has already been made with server
    #         5. Check if this connection results in duplicate server_ids
    #     """
    #     for server in potential_peers:
    #         err, connection= self.attempt_connection_to_peer(server.server, server.port)
    #         connection.host = server.server         # HACK: UNSURE IF NEEDED
    #         if connection.error: # Error of rule 1.
    #             if err == ServErrCode.CANNOT_CONNECT_PEER:
    #                 PeerConnections.show_cannot_connect_peer_msg(f"{server.server}:{server.port}")
    #             connection.sock.close()
    #             continue
    #
    #         err, peer = self.init_peer_connection(connection)
    #         if err == ServErrCode.CANNOT_CONNECT_SELF:
    #             PeerConnections.show_cannot_connect_self_msg()
    #             connection.sock.close()
    #             continue
    #
    #         if peer != None:
    #             with self._peers_lock:
    #                 self.peers.append(peer)
    #
    #             with self._federation_peers_lock:
    #                 # HACK: ^
    #                 self.peers.append(peer)

    # --- Client Management --- #

    def close_client_connection(self, client: ClientConnection) -> bool:
        for c in self.get_clients():
            if client == c:
                c.sock.close()
                return True
        return False

    def get_clients(self) -> list[ClientConnection]:
        with self._clients_lock:
            return list(self.clients)

    def add_client(self, client: ClientConnection) -> None:
        with self._clients_lock:
            self.clients.append(client)

    def remove_client(self, client: ClientConnection) -> None:
        self.clients.remove(client)

    def is_duplicate_client_id(self, id: str) -> bool:
        for client in self.get_clients():
            if client.id == id:
                return True
        return False

    def add_sub_to_client(self, client: ClientConnection, sub: Subscription):
        with self._clients_lock:
            for c in self.clients:
                if c == client:
                    c.subscriptions.append(sub)
                    break

    def remove_sub_from_client(self, topic):
        pass

    def process_msg(self, client: ClientConnection, msg_data: dict):
        try:
            id = msg_data["id"]
            code = msg_data["code"]
            msg_data = msg_data["message"]

            match code:
                case self.messenger.PUBLISH_CODE:
                    topic = msg_data["topic"]
                    message = msg_data["msg"]
                    publishing_server = msg_data["publishing_server"]
                    message_full = f"{topic}: {message} ({publishing_server}:{id})"
                    msg = {
                        "messge_full": message_full,
                        "topic": topic,
                        "msg": message,
                        "comms": f"{publishing_server}:{id}",
                    }
                    self.relay_published_msg(topic, msg)
                    return self.messenger.PUBLISH_CODE
                case self.messenger.SUBCRIBE_CODE:
                    topic = msg_data["topic"]
                    op = msg_data["op"]
                    arg = msg_data["arg"]
                    self.add_sub_to_client(client, Subscription(topic, op=op, arg=arg))

                case self.messenger.DISCON_CODE:
                    return self.messenger.DISCON_CODE
                case self.messenger.SEND_FILE:
                    topic = msg_data["topic"]
                    publishing_server = msg_data["publishing_server"]
                    file_data = msg_data["msg"]

                    msg = {
                        "topic": topic,
                        "msg": file_data,
                        "comms": f"{publishing_server}:{id}"
                    }

                    self.relay_sent_file(topic, msg)
                    return self.messenger.SEND_FILE
                case _:
                    print("unknown msg code")

        except (KeyError, TypeError) as e:
            print(e)
            print("bad client message")

    def start_receiving_from_client(self, client: ClientConnection):
        # TODO: Handling receiving data from a client program
        while True:
            try:
                data = client.sock.recv(4)
                if not data:
                    break

                success, msg_len = self.messenger.decode_len_msg(data)
                if not success:
                    print("protocol error")

                raw_msg = client.sock.recv(msg_len)
                decoded_msg = self.messenger.decode_msg(raw_msg)

                try:
                    msg_data = json.loads(decoded_msg)
                    return_code = self.process_msg(client, msg_data)

                    if return_code == self.messenger.DISCON_CODE:
                        break

                except json.JSONDecodeError as e:
                    print(e)
                    print("bad client message")

            except ConnectionResetError:
                break

        # When exiting loop client would have disconnected
        self.show_client_disconnect(client.id)
        self.close_client_connection(client)
        self.remove_client(client)


    # --- Message Forwarding --- #
    def relay_sent_file(self, topic, msg: dict):
        for c in self.get_clients():
            if c.matches_subscription(topic, msg):
                raw_msg = self.messenger.gen_msg(self.messenger.SEND_FILE, msg)
                self.messenger.send_msg(c.sock, self.messenger.encode_msg(raw_msg))


    def relay_published_msg(self, topic, msg: dict):
        for c in self.get_clients():
            if c.matches_subscription(topic, msg):
                raw_msg = self.messenger.gen_msg(
                  self.messenger.PUBLISH_CODE, msg)
                self.messenger.send_msg(c.sock,
                    self.messenger.encode_msg(raw_msg))


    # --- Connection Handling --- #

    def process_connections(self, listening_connection: Connection):
        listening_socket = listening_connection.sock
        while True:
            client_socket, _ = listening_socket.accept()
            client_handling_thread = Thread(target=self.start_connection,
                                            args=(client_socket,), daemon=True)
            client_handling_thread.start()


    def initialise_connection(self, 
            sock: socket.socket) -> tuple[int, ClientConnection | dict | None]:
        sock.settimeout(0.5) # time to wait to ensure known client
        try:
            data = sock.recv(4)
            if len(data) < 4:
                self.show_unknown_client_msg()
                return (ServErrCode.UNKNOWN_CLIENT_CODE, None)

            success, msg_len = self.messenger.decode_len_msg(data)
            if not success:
                self.show_unknown_client_msg()
                return (ServErrCode.UNKNOWN_CLIENT_CODE, None)

            raw_msg = sock.recv(msg_len)
            decoded_msg = self.messenger.decode_msg(raw_msg)

            try:
                # HACK: Doesn't work for peer connections
                msg_data = json.loads(decoded_msg)
                header = msg_data["header"]
                type_flag = msg_data["type_flag"]
                client_id = msg_data["id"]

                if type_flag == 0:
                    if header != "1588":
                        self.show_unknown_client_msg()
                        return (ServErrCode.UNKNOWN_CLIENT_CODE, None)
                    elif self.is_duplicate_client_id(client_id):
                        self.show_client_duplicate_msg(client_id)
                        return (ServErrCode.DUP_CLIENT_CODE, None)

                    client: ClientConnection = ClientConnection(sock, client_id)
                    return (ServErrCode.OK_CODE, client)
                else:
                    if header != "1588":
                        self.show_unknown_client_msg()
                        return (ServErrCode.UNKNOWN_CLIENT_CODE, None)

                    peer = self.recv_handshake_req_from_peer(sock, msg_data)
                    return (ServErrCode.OK_CODE, None)


            except (json.JSONDecodeError, TypeError, KeyError):
                self.show_unknown_client_msg()
                return (ServErrCode.UNKNOWN_CLIENT_CODE, None)

        except socket.timeout:
            self.show_unknown_client_msg()
            return (ServErrCode.UNKNOWN_CLIENT_CODE, None)


    def start_connection(self, connection_socket: socket.socket):
        # WARNING: DOESN'T WORK FOR PEER SERVERS CONNECTING
        try:
            err, connected_program = self.initialise_connection(connection_socket)
            connection_socket.settimeout(None) # remove timeout used in ^^

            if err == ServErrCode.UNKNOWN_CLIENT_CODE:
                connection_socket.close()
                return
            elif err == ServErrCode.DUP_CLIENT_CODE:
                raw_msg = self.messenger.gen_msg(self.messenger.DUP_ID_CODE)
                self.messenger.send_msg(connection_socket, 
                                        self.messenger.encode_msg(raw_msg))
                connection_socket.close()
                return
            elif connected_program == None:
                connection_socket.close()
                return
            elif isinstance(connected_program, ClientConnection):
                self.show_client_connected_msg(connected_program.id)
                raw_msg = self.messenger.gen_msg(self.messenger.OK_CODE)
                self.messenger.send_msg(connection_socket,
                                        self.messenger.encode_msg(raw_msg))
                self.add_client(connected_program)
                self.start_receiving_from_client(connected_program)
            else:
                print("is peer")

        except Exception as e:  # HACK: This needs changing to be more useful
            print(e, sys.stderr)


    # --- Server Commands Handling --- #
    def handle_user_input(self):
        while True:
            try:
                ready, _, _ = select.select([sys.stdin], [], [], 0.2)
                if ready:
                    server_input = sys.stdin.readline().rstrip().strip()
                    self.process_user_input(server_input)
            except EOFError:
                print("EOF EXCEPTION")
                return

    def process_user_input(self, user_input):
        pass

    # --- Run Handling --- #

    def run(self, conn: Connection):
        # Create thread for handling processing all new connections
        Thread(
            target=self.process_connections,
            args=(conn,),
            daemon=True
        ).start()

        # In main Thread, handle the server input (from user)
        self.handle_user_input()


    # --- Logging --- #
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
                "0.0.0.0", serv_port, socket.AF_INET, socket.SOCK_STREAM
            )
            connection.sock.bind(addr[0][4])
            connection.sock.listen(QUEUED_CONNS)
            connection.port = addr[0][4][1] # port for connection
            return connection

        connection.sock.bind(("0.0.0.0", port))
        connection.sock.listen(QUEUED_CONNS)
        connection.port = connection.sock.getsockname()[1]

    except Exception:
        connection.error = True

    return connection


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
    connection: Connection = attempt_listen(arguments.listenOnPort)
    if connection.error:
        show_error(Errors.UNABLE_TO_LISTEN,
                   port=(arguments.listenOnPort or connection.port))
        exit_program(Errors.UNABLE_TO_LISTEN)

    # TODO: Move to function VV
    print_stderr(f"{PROGRAM}: listening on port {connection.port}") 

    ## Server Runtime Behaviour
    server: PubSubServer = PubSubServer(arguments.server_id, connection.host, connection.port)
    # server.connect_to_peers(arguments.servers)
    server.run(connection)

if __name__ == "__main__":
    main()
















