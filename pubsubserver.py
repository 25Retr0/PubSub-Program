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
from re import split
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
class Commands:

    def __init__(self):
        # Strings
        self.all = "--all"
        self.quit = "/quit"
        self.listclients = "/listclients"
        self.listpeers = "/listpeers"
        self.peer = "/peer"
        self.limit = "/limit"

    def show_unknown_command_msg(self) -> None:
        print_stderr(f"{PROGRAM}: unknown command")

    def show_unknown_argumemts_msg(self, command: str) -> None:
        print_stderr(f"{PROGRAM}: unknown argument(s) - usage: " \
            f"{self.get_usage_cmd(command)}")

    def show_no_clients_connected(self):
        print_stdout(f"{PROGRAM}: No clients connected")

    def show_no_peers_connected(self):
        print_stdout(f"{PROGRAM}: No peer servers connected")

    def show_client_id_unknown(self, client_id: str):
        print_stderr(f"{PROGRAM}: Client \"{client_id}\" is unknown")

    def show_invalid_topic(self, topic: str):
        print_stderr(f"{PROGRAM}: Topic \"{topic}\" is not valid")

    def show_rate_limit_range(self):
        print_stderr(f"{PROGRAM}: Rate limit must be 0 to 3600 seconds inclusive")

    def get_usage_cmd(self, command: str) -> str:
        match command:
            case self.quit: return f"{self.quit}"
            case self.listclients: return f"{self.listclients} [--all]"
            case self.listpeers: return f"{self.listpeers} [--all]"
            case self.peer: return f"{self.peer} [server]:port"
            case self.limit: return f"{self.limit} clientid topic N"
            case _: return "Unknown usage"

class ServerProgramArgs:
    def __init__(self):
        self.server_id: str = ""
        self.listenOnPort: Optional[str] = None
        self.servers: list[Server] = []
        self.error: bool = False


class ClientConnection():
    def __init__(self, sock: socket.socket, id: str, serv_id):
        self.sock = sock
        self.id = id
        self.subscriptions: list[Subscription] = []
        self.server_id = serv_id

    def __eq__(self, other) -> bool:
        if isinstance(other, ClientConnection):
            return self.id == other.id
        return False

    def get_subscriptions(self):
        topics = []
        for sub in self.subscriptions:
            topics.append(sub.topic)
        return topics 

    def remove_subscriptions(self, topic):
        for i, sub in enumerate(self.subscriptions):
            if sub.topic == topic:
                self.subscriptions.pop(i)

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
    def __init__(self, server_id: str, port):
        self.id = server_id
        self.ip = "localhost"
        self.port = port
        self.uid = str(uuid4())
        self.messenger = MessageProtocol(is_server=True, id=self.id, uid=self.uid)
        self.commands = Commands()

        self.did_quit = False
        self._did_quit_lock = Lock()
        
        self.clients: list[ClientConnection] = []
        self._clients_lock = Lock()

        self.peers = []
        # self.peers = [
        # { "id": "Server_A", "socket": <socket_obj>, "address": "127.0.0.1:5000", uid: "uuid-111" },
        # ]
        self._peers_lock = Lock()

        self.peer_federation_map = {}
        # self.federation_map = {
        #     "Server_A": "uuid-111",
        # }
        self._peer_federation_map_lock = Lock()

        self.client_federation_map = {}
        # self.client_federation_map = {
        #     "Server_A": "Client_A",
        # }
        self._client_federation_map_lock = Lock()

        # NOTE: current plan for notifying -> have list of the servers that have seen this
        # somewhere in the messge. And then if you haven't seen this, or have peers that haven't
        # seen this, then record or pass it in. If you dont meet either of those, just drop packet
        # Last updated timestamp on packet notifiers. To make sure it's the most recent data

    def set_did_quit(self, boolean):
        with self._did_quit_lock:
            self.did_quit = boolean
        
    def did_server_quit(self):
        with self._did_quit_lock:
            did = self.did_quit
        return did

    # --- Peer Management --- #

    def list_peers(self):
        with self._peers_lock:
            local_peers = self.peers
            dislay_list = []
            for peer in self.peers:
                dislay_list.append(peer["id"])

        sorted(dislay_list)

        if len(dislay_list) != 0:
            for id in dislay_list:
                print_stdout(id)
        else:
            self.commands.show_no_peers_connected()

    def list_peers_all(self):
        pass


    def remove_peer(self, peer):
        with self._peers_lock:
            self.peers.remove(peer)
    
    def attempt_connection_to_peer(self, server: str, serv_port: str) -> tuple[int, Connection]:
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        connection: Connection = Connection(sock)

        try:
            if serv_port.isdigit():
                port = int(serv_port)
            else:
                port = serv_port

            if server == "": server = "localhost"

            serverAddr = socket.getaddrinfo(
                server, port, socket.AF_INET, socket.SOCK_STREAM
            )

            if self.ip == server and port == self.port:
                connection.error = True
                return (ServErrCode.CANNOT_CONNECT_SELF, connection)

            connection.sock.connect(serverAddr[0][4])
            connection.port = serverAddr[0][4][1]

        except Exception as e:
            connection.error = True
            return (ServErrCode.CANNOT_CONNECT_PEER, connection)

        return (ServErrCode.OK_CODE, connection)


    def init_peer_connection(self, conn: Connection):
        # A server asking for a peer connection
        # Handle like the server is a client

        # Send my data to the peer im connecting to
        with self._peers_lock:
            known_peers = self.peers

        msg = self.messenger.gen_msg(self.messenger.CONN_CODE, {"known_peers": known_peers})
        msg = self.messenger.encode_msg(msg)
        self.messenger.send_msg(conn.sock, msg)

        conn.sock.settimeout(0.5)
        # Check im getting data back, whether there's a naming conflict etc
        try:
            data = conn.sock.recv(4)
            if len(data) < 4:
                return ServErrCode.INCOMPATIBLE_PEER, None
            success, msg_len = self.messenger.decode_len_msg(data)
            if not success:
                return ServErrCode.INCOMPATIBLE_PEER, None

            raw_msg = conn.sock.recv(msg_len)
            decoded_msg = self.messenger.decode_msg(raw_msg)
            try:
                msg_data = json.loads(decoded_msg)
                id = msg_data["id"]
                uid = msg_data["uid"]
                code = msg_data["code"]

                if code == self.messenger.PEER_SELF_ID:
                    return ServErrCode.CANNOT_CONNECT_SELF, None
                elif code == self.messenger.PEER_DIRECTLY_CONN:
                    return ServErrCode.DIRECTLY_CONNECTED_PEER, None
                elif code == self.messenger.PEER_NAME_CLASH:
                    return ServErrCode.DUP_SERVER_ID, None

                peer = {
                    "id": id,
                    "uid": uid,
                    "socket": conn.sock,
                    "host": conn.host,
                    "port": conn.port
                }
                return ServErrCode.OK_CODE, peer

            except:
                return ServErrCode.INCOMPATIBLE_PEER, None

        except socket.timeout:
            return ServErrCode.INCOMPATIBLE_PEER, None


    def connect_to_peers(self, potential_peers: list[Server]):
        """Attempt to connect to any peer servers given 'peer_servers'.
        Will attempt to do:
            1. Connect to the given server
            2. Validate it is communicating with a valid server
            3. Check peer is not itself
            4. Check if a connection has already been made with server
            5. Check if this connection results in duplicate server_ids
        """
        for server in potential_peers:
            err, connection = self.attempt_connection_to_peer(server.server, server.port)
            connection.host = server.server         # HACK: UNSURE IF NEEDED
            argvalue = f"{server.server}:{server.port}"
            if connection.error: # Error of rule 1.
                if err == ServErrCode.CANNOT_CONNECT_PEER:
                    PeerConnections.show_cannot_connect_peer_msg(f"{server.server}:{server.port}")
                elif err == ServErrCode.CANNOT_CONNECT_SELF:
                    PeerConnections.show_cannot_connect_self_msg()
                connection.sock.close()
                continue

            err, peer = self.init_peer_connection(connection)
            connection.sock.settimeout(None)
            if err == ServErrCode.INCOMPATIBLE_PEER:
                PeerConnections.show_incompatible_peer_msg(argvalue)
                connection.sock.close()
                continue
            elif err == ServErrCode.CANNOT_CONNECT_SELF:
                PeerConnections.show_cannot_connect_self_msg()
                connection.sock.close()
                continue
            elif err == ServErrCode.DIRECTLY_CONNECTED_PEER:
                PeerConnections.show_already_connected_peer_msg(argvalue)
                connection.sock.close()
                continue
            elif err == ServErrCode.DUP_SERVER_ID:
                PeerConnections.show_dupe_server_peer_id_msg(argvalue)
                connection.sock.close()
                continue
            elif err == ServErrCode.OK_CODE and peer != None:
                with self._peers_lock:
                    self.peers.append(peer)
                PeerConnections.show_peer_connected_msg(argvalue, peer["id"])
                Thread(target=self.start_receiving_from_peer,
                       args=(peer,), daemon=True).start()
            else:
                print("eer: ", err)



    # --- Client Management --- #

    def list_clients(self):
        local_clients = self.get_clients()
        display_list = []
        for client in local_clients:
            serv_id = client.server_id
            id = client.id
            display_list.append(f"{serv_id}:{id}")

        sorted(display_list) # sorts by ASCII sort order

        if len(display_list) != 0:
            for id in display_list:
                print_stdout(id)
        else:
            self.commands.show_no_clients_connected()


    def list_clients_all(self):
        all_clients = self.get_all_clients()


    def close_client_connection(self, client: ClientConnection) -> bool:
        for c in self.get_clients():
            if client == c:
                c.sock.close()
                return True
        return False

    def get_clients(self) -> list[ClientConnection]:
        with self._clients_lock:
            return list(self.clients)

    def get_all_clients(self):
        with self._client_federation_map_lock:
            for mapping in self.client_federation_map.items():
                pass


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

    def remove_sub_from_client(self, client: ClientConnection, topic: str):
        with self._clients_lock:
            for c in self.clients:
                if c == client:
                    c.remove_subscriptions(topic)


    def process_peer_msg(self, peer, msg_data):
        try:
            code = msg_data["code"]
            match code:
                case self.messenger.PEER_DISCON:
                    return self.messenger.PEER_DISCON
                case _:
                    print("unknown code")

        except (KeyError, TypeError) as e:
            print(e)
            print("bad client message")


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
                case self.messenger.UNSUBCRIBE_CODE:
                    topic = msg_data["topic"]

                    for c in self.get_clients():
                        if c.id == id:
                            self.remove_sub_from_client(c, topic)

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
        while not self.did_server_quit():
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

    def start_receiving_from_peer(self, peer):
        while not self.did_server_quit():
            try:
                data = peer["socket"].recv(4)
                if not data:
                    self.show_peer_disconnected(peer["id"])
                    break;

                success, msg_len = self.messenger.decode_len_msg(data)
                if not success:
                    print("protocol errro")

                raw_msg = peer["socket"].recv(msg_len)
                decoded_msg = self.messenger.decode_msg(raw_msg)

                try:
                    msg_data = json.loads(decoded_msg)
                    return_code = self.process_peer_msg(peer, msg_data)

                    if return_code == self.messenger.PEER_DISCON:
                        self.show_peer_shutdown_warning(msg_data["id"])
                        break;

                except json.JSONDecodeError as e:
                    print(e)
                    print("bad client message")

            except (ConnectionResetError, BrokenPipeError):
                self.show_peer_disconnected(peer["id"])
                break;

        # remove peer connection from self._peers and then federation
        with self._peers_lock:
            self.peers.remove(peer)


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
        while not self.did_server_quit():
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

                    client: ClientConnection = ClientConnection(sock, client_id, self.id)
                    return (ServErrCode.OK_CODE, client)
                else:
                    if header != "1588":
                        self.show_unknown_client_msg()
                        return (ServErrCode.UNKNOWN_CLIENT_CODE, None)

                    peer = {
                        "id": msg_data["id"],
                        "uid": msg_data["uid"],
                        "socket": sock,
                        "known_peers": msg_data["message"]["known_peers"]
                    }

                    # check if name matches with me
                    if self.id == peer["id"] and self.uid == peer["uid"]:
                        return (ServErrCode.CANNOT_CONNECT_SELF, None)
                    elif self.id == peer["id"]:
                        return (ServErrCode.DUP_SERVER_ID, None)

                    # Check if new peer is directly connected to me
                    with self._peers_lock:
                        for p in self.peers:
                            if p["id"] == peer["id"] and p["uid"] != peer["uid"]:
                                return (ServErrCode.DUP_SERVER_ID, None)
                            elif p["uid"] == peer["uid"]:
                                return (ServErrCode.DIRECTLY_CONNECTED_PEER, None)
                            else:
                                # check if any known peer's of new peer clash names
                                for kp in peer["known_peers"]:
                                    if p["id"] == kp["id"] and p["uid"] != kp["uid"]:
                                        return (ServErrCode.DUP_SERVER_ID, None)

                    return (ServErrCode.OK_CODE, peer)


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
            elif err == ServErrCode.DIRECTLY_CONNECTED_PEER:
                raw_msg = self.messenger.gen_msg(self.messenger.PEER_DIRECTLY_CONN)
                self.messenger.send_msg(connection_socket, self.messenger.encode_msg(raw_msg))
                connection_socket.close()
            elif err == ServErrCode.DUP_SERVER_ID:
                raw_msg = self.messenger.gen_msg(self.messenger.PEER_NAME_CLASH)
                self.messenger.send_msg(connection_socket, self.messenger.encode_msg(raw_msg))
                connection_socket.close()
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
                raw_msg = self.messenger.gen_msg(self.messenger.OK_CODE)
                self.messenger.send_msg(connection_socket, self.messenger.encode_msg(raw_msg))
                with self._peers_lock:
                    self.peers.append(connected_program)
                    PeerConnections.show_received_peer_connection_msg(connected_program["id"])
                self.start_receiving_from_peer(connected_program)

        except Exception as e:  # HACK: This needs changing to be more useful
            print(e, sys.stderr)


    # --- Server Commands Handling --- #
    def handle_user_input(self):
        while not self.did_server_quit():
            try:
                ready, _, _ = select.select([sys.stdin], [], [], 0.2)
                if ready:
                    server_input = sys.stdin.readline().rstrip().strip()
                    self.process_user_input(server_input)
            except EOFError:
                print("EOF EXCEPTION")
                return

    def process_user_input(self, user_input):
        # /quit
        if user_input.startswith(self.commands.quit):
            quit_info = split_args(user_input)
            if len(quit_info) == 1:
                # Notify peers and clients
                with self._peers_lock:
                    for peer in self.peers:
                        msg = self.messenger.gen_msg(self.messenger.PEER_DISCON)
                        msg = self.messenger.encode_msg(msg)
                        self.messenger.send_msg(peer["socket"], msg)
                        peer["socket"].close()

                with self._clients_lock:
                    for client in self.clients:
                        msg = self.messenger.gen_msg(self.messenger.DISCON_CODE)
                        msg = self.messenger.encode_msg(msg)
                        self.messenger.send_msg(client.sock, msg)
                        client.sock.close()

                self.set_did_quit(True)
                return
            else:
                self.commands.show_unknown_argumemts_msg(self.commands.quit)
                return

        # /listclients [-all]
        elif user_input.startswith(self.commands.listclients):
            list_info = split_args(user_input)
            if not (len(list_info) == 1 or len(list_info) == 2):
                self.commands.show_unknown_argumemts_msg(self.commands.listclients)
                return

            if len(list_info) == 1:
                self.list_clients()
            elif len(list_info) == 2 and list_info[1] == self.commands.all:
                self.list_clients_all()
            else:
                self.commands.show_unknown_argumemts_msg(self.commands.listclients)

        # /listpeers [-all]
        elif user_input.startswith(self.commands.listpeers):
            list_info = split_args(user_input)
            if not (len(list_info) == 1 or len(list_info) == 2):
                self.commands.show_unknown_argumemts_msg(self.commands.listpeers)
                return

            if len(list_info) == 1:
                self.list_peers()
            elif len(list_info) == 2 and list_info[1] == self.commands.all:
                self.list_peers_all()
            else:
                self.commands.show_unknown_argumemts_msg(self.commands.listpeers)

        # /peer [server]:port
        elif user_input.startswith(self.commands.peer):
            peer_info = split_args(user_input)

            if len(peer_info) != 2:
                self.commands.show_unknown_argumemts_msg(self.commands.peer)
                return

            server_port = peer_info[1]
            if ":" not in server_port:
                self.commands.show_unknown_argumemts_msg(self.commands.peer)
                return
            server_port = server_port.split(":")
            if not (len(server_port) == 1 or len(server_port) == 2):
                self.commands.show_unknown_argumemts_msg(self.commands.peer)
                return

            if len(server_port) == 2:
                server, port = server_port[0], server_port[1]
            else:
                server, port = "", server_port[0]

            port = port.strip()
            server = server.strip()
            if not port:
                self.commands.show_unknown_argumemts_msg(self.commands.peer)
                return


            # attempt connections
            peer_server = Server(port)
            if server != "":
                peer_server.server = server
            self.connect_to_peers([peer_server])

        # /limit clientid topic N
        elif user_input.startswith(self.commands.limit):
            limit_info = split_args(user_input)

            if len(limit_info) != 4:
                self.commands.show_unknown_argumemts_msg(self.commands.limit)
                return

            clientid = limit_info[1]
            topic = limit_info[2]
            n = limit_info[3]


            found_client = False
            with self._clients_lock:
                for client in self.clients:
                    if client.id == clientid:
                        found_client = True
                        client_sock = client.sock

            if not found_client:
                print_stderr(f"{PROGRAM}: Client \"{clientid}\" is unknown")
                return

            if not is_valid_topic(topic):
                self.commands.show_invalid_topic(topic)
                return

            try:
                n = int(n)
                if not (0 <= n <= 3600):
                    print_stderr(f"{PROGRAM}: Rate limit must be 0 to 3600 seconds inclusive")
                    return
            except:
                print_stderr(f"{PROGRAM}: Rate limit must be 0 to 3600 seconds inclusive")
                return

            message = {
                "topic": topic,
                "n": n
            }
            msg = self.messenger.gen_msg(self.messenger.LIMIT_CODE, message)
            msg = self.messenger.encode_msg(msg)
            self.messenger.send_msg(client_sock, msg)

        # unknown
        else:
            self.commands.show_unknown_command_msg()
            return

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

            s: Server = Server(port, server=(server))
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
    server: PubSubServer = PubSubServer(arguments.server_id, connection.port)
    server.connect_to_peers(arguments.servers)
    server.run(connection)

if __name__ == "__main__":
    main()
















