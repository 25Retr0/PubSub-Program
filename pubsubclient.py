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
    OPTION_ARGUMENT = "--"

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

### Data Classes ###############################################################

@dataclass()
class ClientProgramArgs:
    port: int = -1
    clientid: str = "PLACEHOLDER"
    topic: Optional[str] = None
    server: Optional[str] = None
    message: Optional[str] = None

### Functions ##################################################################
def print_stderr(message: str):
    """Helper method for printing a message to stderr."""
    print(message, file=sys.stderr)


def print_stdout(message: str):
    """Helper method for printing a message to stdout."""
    print(message, file=sys.stdout)


def show_error(error_code: int, **kwargs):
    """Given an error code, print the matching message"""
    match error_code:
        case Errors.USAGE_ERROR_CODE:
            print_stderr(Errors.usage_msg())
        case Errors.BAD_CLIENT_ID_CODE:
            msg = kwargs.get("clientid", "ClientID")
            print_stderr(Errors.bad_client_id_msg(msg))


def parse_arguments(arguments: list[str]) -> ClientProgramArgs:
    """ arugments: [--topic topic] [server]:port clientid [message]

    All arguments must happen in this order, if they are specified.
    """

    program_args: ClientProgramArgs = ClientProgramArgs()
    print(arguments)
    
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
