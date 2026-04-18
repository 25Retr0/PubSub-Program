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

### Constants ##################################################################
class Constants:
    OPTION = "--"
    COLON = ":"
    TOPIC = "--topic"
    MIN_ARGS = 2
    MAX_ARGS = 5

class Errors:
    USAGE_ERROR_CODE = 1
    BAD_CLIENT_ID_CODE = 4

    @staticmethod
    def usage_msg()-> str:
        return f"Usage: pubsubclient [--topic topic] [server]:port " \
                "clientid [message]"

    @staticmethod
    def bad_client_id_msg(clientid: str) -> str:
        return f"pubsubclient: bad client ID \"{clientid}\""

    @staticmethod
    def unknown_error_msg() -> str:
        return f"pubsubclient: Unknown Error Detected"

### Data Classes ###############################################################

@dataclass()
class ClientProgramArgs:
    topic: Optional[str] = None
    server: str = "localhost"
    port: str | int = -1
    clientid: str = "PLACEHOLDER"
    message: Optional[str] = None

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
            msg = kwargs.get("clientid", "ClientID")
            print_stderr(Errors.bad_client_id_msg(msg))
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

    # At most 4 arguments and at least 2 arguments
    if not (Constants.MIN_ARGS <= len(arguments) <= Constants.MAX_ARGS):
        show_error(Errors.USAGE_ERROR_CODE)
        exit_program(Errors.USAGE_ERROR_CODE)

    # parse [--topic topic]
    # if first argument starts with --, then you need atleast 3 more args
    args: int = 0
    if arguments[args] == Constants.TOPIC:
        # cannot fail as MIN_ARGS == 2 and len(arguments) > 2
        args += 1
        program_args.topic = arguments[args]

        # if there are not 2 more arguments atleast, then just exit
        if (len(arguments) - args) < 2:
            show_error(Errors.USAGE_ERROR_CODE)
            exit_program(Errors.USAGE_ERROR_CODE)

        args += 1   # increment for next argument

    elif arguments[args].startswith(Constants.OPTION):
        show_error(Errors.USAGE_ERROR_CODE)
        exit_program(Errors.USAGE_ERROR_CODE)

    # parse [server]:port
    if arguments[args].startswith(Constants.OPTION):
        show_error(Errors.USAGE_ERROR_CODE)
        exit_program(Errors.USAGE_ERROR_CODE)

    server, port = arguments[args].split(Constants.COLON)
    args += 1

    if server.strip(): # not empty
        program_args.server = server.strip()

    if not port.strip(): # is empty
        show_error(Errors.USAGE_ERROR_CODE)
        exit_program(Errors.USAGE_ERROR_CODE)

    program_args.port = port

    # parse clientid

    if arguments[args].startswith(Constants.OPTION):
        show_error(Errors.USAGE_ERROR_CODE)
        exit_program(Errors.USAGE_ERROR_CODE)

    clientid = arguments[args].strip()
    if not clientid:
        show_error(Errors.USAGE_ERROR_CODE)
        exit_program(Errors.USAGE_ERROR_CODE)

    program_args.clientid = clientid

    if len(arguments) - args == 1: # 1 argument left
        if arguments[args].startswith(Constants.OPTION):
            show_error(Errors.USAGE_ERROR_CODE)
            exit_program(Errors.USAGE_ERROR_CODE)

        program_args.message = arguments[len(arguments) - 1]

    return program_args


def run_client():
    pass


### Main #######################################################################

def main():

    ## Command line argument parsing
    arguments: ClientProgramArgs = parse_arguments(sys.argv[1:])

    ## Client ID Checking

    ## Topic Checking

    ## Message Checking

    ## Connection Checking

    ## Server Validity Checking

    ## Client Uniqueness Checking

    ## Client Runtime Behaviour
    run_client()


    pass

if __name__ == "__main__":
    main()
