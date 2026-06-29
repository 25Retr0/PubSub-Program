from threading import Thread

from Subscriptions import SubscriptionManager
from CommandHandler import ClientCommandHandler
from CLI import ClientCLI

class Client:

    def __init__(self, client_id, connection):
        self._identifier = client_id
        self.connection = connection

        self.subscriptions = SubscriptionManager()
        self.commands = ClientCommandHandler()
        self.interface = ClientCLI()


    def read_user_input(self):
        print("reading...")
        return ""

    def handle_user_input(self, args):
        print("handling...")
        return 1

    def receive_from_server(self, server_connection):
        return 0

    def read_server_message(self):
        pass

    def handle_server_message(self):
        pass

    def run(self, connection):
        Thread(
            target=self.receive_from_server,
            args=(connection,),
            daemon=True
        ).start()

        while True:
            user_input = self.read_user_input()
            error_code = self.handle_user_input(user_input)
            if error_code != 0:
                break
