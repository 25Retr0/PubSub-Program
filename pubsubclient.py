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

    arg: int = 0
    
    # if --topic exists -- MIN_ARGS + 2 is needed.
    args_len = len(arguments)
    if arguments[arg] == "--topic" and args_len >= 4:
        arg += 1
        topic_arg = arguments[arg]

        if topic_arg.strip() == '':
            show_error(Errors.USAGE_ERROR_CODE)
            exit_program(Errors.USAGE_ERROR_CODE)

        program_args.topic = topic_arg
        arg += 1 # move to next argument
    elif (arguments[arg] == "--topic" and args_len < 4):
        show_error(Errors.USAGE_ERROR_CODE)
        exit_program(Errors.USAGE_ERROR_CODE)
    elif (arguments[arg].startswith("--")):
        show_error(Errors.USAGE_ERROR_CODE)
        exit_program(Errors.USAGE_ERROR_CODE)

    # Now check if length of args is within MAX_ARGS (5)
    if (args_len > 5):
        show_error(Errors.USAGE_ERROR_CODE)
        exit_program(Errors.USAGE_ERROR_CODE)

    # NOTE: While parsing no args (except [message]) can start with '--'

    # Parsing required arguments --> [server]:port clientid
    # First check string contains ":"
    if not (":" in arguments[arg]):
        show_error(Errors.USAGE_ERROR_CODE)
        exit_program(Errors.USAGE_ERROR_CODE)

    server, port = arguments[arg].split(":")
    port = port.strip()
    server = server.strip()
    if (not port or server.startswith("--")): 
        show_error(Errors.USAGE_ERROR_CODE)
        exit_program(Errors.USAGE_ERROR_CODE)

    if server != '':    # Check it was given as it is optional
        program_args.server = server

    program_args.port = port

    arg += 1
    clientid = arguments[arg].strip()
    if not clientid or clientid.startswith("--"):
        show_error(Errors.USAGE_ERROR_CODE)
        exit_program(Errors.USAGE_ERROR_CODE)
    program_args.clientid = clientid

    arg += 1

    if (args_len == 5 and program_args.topic != None):
        # allow message argument
        program_args.message = arguments[arg].strip()
    elif (args_len >= 3 and program_args.topic == None):
        # no topic was given do not allow message
        show_error(Errors.USAGE_ERROR_CODE)
        exit_program(Errors.USAGE_ERROR_CODE)

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
