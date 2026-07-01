from threading import Thread
import select
import sys

from Subscriptions import SubscriptionManager
from CommandHandler import ClientCommandHandler
from CLI import ClientCLI

class Client:

    def __init__(self, client_id, connection):
        self.identifier = client_id
        self.connection = connection

        self.subscriptions = SubscriptionManager()
        # self.rate_limiter = RateLimiter()
        # self.file_handler = FileHandler()
        self.commands = ClientCommandHandler(self)
        self.interface = ClientCLI()

        self._running = False
        self._error_code = 0

    def quit(self) -> None:
        self._running = False
        self._error_code = 0

    def read_user_input(self):
        try:
            ready, _, _ = select.select([sys.stdin], [], [], 0.2)
            if ready:
                return sys.stdin.readline().strip()
        except (EOFError, KeyboardInterrupt) as e:
            print(f"Exception in read_user_input, exiting...")
            self._running = False
            self._error_code = 1
            return ""

    def handle_user_input(self, args: str):
        cmd, cmd_args = self.commands.parse_command(args)
        if not cmd and not cmd_args:
            return

        self.commands.handle_command(cmd, cmd_args)

    def receive_from_server(self):
        pass

    def read_server_message(self):
        pass

    def handle_server_message(self):
        pass

    def run(self):
        self._running = True

        Thread(
            target=self.receive_from_server,
            args=(),
            daemon=True
        ).start()

        while self._running:
            user_input = self.read_user_input()
            if not user_input:
                continue    # Nothing to process so just loop again
            print(f"ECHO: {user_input}")

            self.handle_user_input(user_input)
            if self._error_code != 0:
                break

        print(f"exiting with code: {self._error_code}")
        return self._error_code
