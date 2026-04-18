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
    
    # Quick check - max and min number of args
    args_len = len(arguments)
    if not (Constants.MIN_ARGS <= args_len <= Constants.MAX_ARGS):
        show_error(Errors.USAGE_ERROR_CODE)
        exit_program(Errors.USAGE_ERROR_CODE)


    program_args: ClientProgramArgs = ClientProgramArgs()

    # Parsing required arguments --> [server]:port clientid
    arg: int = 0
    server, port = arguments[arg].split(Constants.COLON)
    if server.strip(): # Server is not empty
        program_args.server = server.strip()

    if not port.strip(): # if port is empty
        show_error(Errors.USAGE_ERROR_CODE)
        exit_program(Errors.USAGE_ERROR_CODE)
    program_args.port = port.strip()

    arg += 1
    clientid = arguments[arg].strip()
    if not clientid:
        show_error(Errors.USAGE_ERROR_CODE)
        exit_program(Errors.USAGE_ERROR_CODE)
    program_args.clientid = clientid

    print(program_args)
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
