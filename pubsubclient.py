""" ! @file pubsubclient.py
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
from dataclasses import dataclass
from typing import Optional

### Error Handler ##############################################################
class Errors:
    USAGE_ERROR_CODE = 1
    BAD_CLIENT_ID_CODE = 4
    INVALID_TOPIC_CODE = 7

    @staticmethod
    def usage_msg()-> str:
        return f"Usage: pubsubclient [--topic topic] [server]:port " \
                "clientid [message]"

    @staticmethod
    def bad_client_id_msg(clientid: str) -> str:
        return f"pubsubclient: bad client ID \"{clientid}\""

    @staticmethod
    def invalid_topic_msg(topic: str) -> str:
        return f"pubsubclient: invalid topic string \"{topic}\""

    @staticmethod
    def unknown_error_msg() -> str:
        return f"pubsubclient: Unknown Error Detected"

### Data Classes ###############################################################
@dataclass()
class ClientProgramArgs:
    topic: Optional[str] = None
    server: str = "localhost"
    port: str | int = -1
    client_id: str | int = "PLACEHOLDER"
    message: Optional[str] = None
    error: bool = False

### Functions ##################################################################
def print_stderr(message: str) -> None:
    """Helper method for printing a message to stderr."""
    print(message, file=sys.stderr)


def print_stdout(message: str) -> None:
    """Helper method for printing a message to stdout."""
    print(message, file=sys.stdout)


def show_error(error_code: int, **kwargs) -> None:
    """Given an error code, print the matching message"""
    match error_code:
        case Errors.USAGE_ERROR_CODE:
            print_stderr(Errors.usage_msg())
        case Errors.BAD_CLIENT_ID_CODE:
            msg = kwargs.get("client_id", "ClientID")
            print_stderr(Errors.bad_client_id_msg(msg))
        case Errors.INVALID_TOPIC_CODE:
            msg = kwargs.get("topic", "TOPIC")
            print_stderr(Errors.invalid_topic_msg(msg))
        case _:
            print_stderr(Errors.unknown_error_msg())

def exit_program(error_code: int) -> None:
    """Exit from program with given error_code."""
    print_stderr(f"\nExited with code '{error_code}'")
    sys.exit(error_code)


def parse_arguments(arguments: list[str]) -> ClientProgramArgs:
    """ arugments: [--topic topic] [server]:port clientid [message]
    """
    program_args: ClientProgramArgs = ClientProgramArgs()

    arg: int = 0
    
    # if --topic exists -- MIN_ARGS + 2 is needed.
    args_len = len(arguments)
    if arguments[arg] == "--topic" and args_len >= 4:
        arg += 1
        topic_arg = arguments[arg]

        if topic_arg.strip() == '':
            program_args.error = True

        program_args.topic = topic_arg
        arg += 1 # move to next argument
    elif ((arguments[arg] == "--topic" and args_len < 4)
        or arguments[arg].startswith("--")):
        program_args.error = True

    # Now check if length of args is within MAX_ARGS (5)
    if (args_len > 5):
        program_args.error = True

    # Parsing required arguments --> [server]:port clientid
    # First check string contains ":"
    if not (":" in arguments[arg]):
        program_args.error = True

    server, port = arguments[arg].split(":")
    port = port.strip()
    server = server.strip()
    if (not port or server.startswith("--")): 
        program_args.error = True

    if server != '':    # Check it was given as it is optional
        program_args.server = server

    program_args.port = port

    arg += 1
    client_id = arguments[arg].strip()
    if not client_id:
        program_args.error = True
    program_args.client_id = client_id

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


def isValidClientId(program_args: ClientProgramArgs) -> bool:
    """Given a ClientProgramArgs object: 
        - take ClientProgramArgs.client_id
        - check it is an integer between 2 and 32 (inclusive).
      Updates ClientProgramArgs.client_id if condition is met and returns True,
      otherwise False
    """
    try:
        client_id = int(program_args.client_id)
        if 2 <= client_id <= 32:
            program_args.client_id = client_id
            return True

        return False
    except ValueError:
        return False


def isValidTopic(topic: str) -> bool:
    """ A valid topic string consists of:
        - Start with a letter (upper or lower)
        - consist of letters, numbers, spaces, and/or '/' (forward slash)
        Returns True if conditions met, otherwise False
    """

    return False

### Main #######################################################################

def main():

    ## Command line argument parsing
    arguments: ClientProgramArgs = parse_arguments(sys.argv[1:])
    if arguments.error:
        show_error(Errors.USAGE_ERROR_CODE)
        exit_program(Errors.USAGE_ERROR_CODE)

    ## Client ID Checking
    if not isValidClientId(arguments):
        show_error(Errors.BAD_CLIENT_ID_CODE, client_id=arguments.client_id)
        exit_program(Errors.BAD_CLIENT_ID_CODE)

    ## Topic Checking
    if arguments.topic != None and not isValidTopic(arguments.topic):
        show_error(Errors.INVALID_TOPIC_CODE, topic=arguments.topic)
        exit_program(Errors.INVALID_TOPIC_CODE)

    ## Message Checking

    ## Connection Checking

    ## Server Validity Checking

    ## Client Uniqueness Checking

    ## Client Runtime Behaviour


    pass

if __name__ == "__main__":
    main()
