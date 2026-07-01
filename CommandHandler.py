###############################################################################
class ClientCommandHandler:

    def __init__(self, client):
        self.commands = CommandMessages()
        self.client = client
        self.interface = client.get_interface()

        self.dispatch = {
            self.commands.quit: self.quit_program,
            self.commands.topic: self.set_topic,
            self.commands.subscribe: self.subscribe,
            self.commands.unsubscribe: self.unsubscribe,
            self.commands.listsubs: self.list_subscriptions,
            self.commands.listlimits: self.list_limits,
            self.commands.publish: self.publish_message,
            self.commands.sendfile: self.send_file
        }


    def split_args(self, text: str, delimiter=" ", quote_char='"') -> list[str]:
        result = []
        current_token = ""
        in_quotes = False
        has_content = False 

        for char in text:
            if char == quote_char:
                in_quotes = not in_quotes
                has_content = True 
            elif char == delimiter and not in_quotes:
                if has_content:
                    result.append(current_token)
                    current_token = ""
                    has_content = False
            else:
                current_token += char
                has_content = True

        if has_content:
            result.append(current_token)

        return result

    def parse_command(self, user_command):
        command_info = self.split_args(user_command)
        if not command_info:
            return (None, None)

        return (command_info[0], command_info[1:])

    def handle_command(self, cmd, args) -> bool:
        if not cmd in self.dispatch:
            #self.interface.display_error(self.commands.unknown_command_msg())
            return False

        command_func = self.dispatch[cmd]
        return command_func(args)

    def quit_program(self, args) -> bool:
        if args:
            return False

        self.client.quit()
        return True

    def set_topic(self, args) -> bool:
        return False

    def subscribe(self, args) -> bool:
        return False

    def unsubscribe(self, args) -> bool:
        return False

    def list_subscriptions(self, args) -> bool:
        return False

    def list_limits(self, args) -> bool:
        return False

    def publish_message(self, args) -> bool:
        return False

    def send_file(self, args) -> bool:
        return False

###############################################################################
class CommandMessages:

    def __init__(self):
        self.quit = "/quit"
        self.topic = "/topic"
        self.subscribe = "/subscribe"
        self.unsubscribe = "/unsubscribe"
        self.listsubs = "/listsubs"
        self.listlimits = "/listlimits"
        self.publish = "/publish"
        self.sendfile = "/sendfile"

    def unknown_command_msg(self) -> str:
        return f"unknown command"

    def unknown_arguments_msg(self, command: str) -> str:
        return f"unknown argument(s) - usage: {self.get_usage_cmd(command)}" 

    def no_topic_msg(self) -> str:
        return f"no default topic set"

    def invalid_filter_msg(self, filter: str) -> str:
        return f"invalid filter string \"{filter}\""

    def indentical_subscription_msg(self) -> str:
        return f"identical subscription ignored"

    def successful_unsubscribe_msg(self, topic: str) -> str:
        return f"unsubscribed from messges about \"{topic}\""

    def failed_unsubscribe_msg(self, topic: str) -> str:
        return f"not subscribed to messages about \"{topic}\""

    def unable_to_open_file_msg(self, filename: str) -> str:
        return f"unable to open file \"{filename}\""

    def invalid_message_msg(self) -> str:
        return f"messages must only contain printable characters"

    def invalid_topic_msg(self, topic: str) -> str:
        return f"invalid topic string \"{topic}\""

    def get_usage_cmd(self, command: str) -> str:
        match command:
            case self.subscribe: return f"{self.subscribe} topic [filter]"
            case self.unsubscribe: return f"{self.unsubscribe} topic"
            case self.topic: return f"{self.topic} topic"
            case self.publish: return f"{self.publish} topic message"
            case self.listsubs: return f"{self.listsubs}"
            case self.sendfile: return f"{self.sendfile} filename [topic]"
            case self.quit: return f"{self.quit}"
            case self.listlimits: return f"{self.listlimits}"
            case _: return "Command unknown... no usage information"

