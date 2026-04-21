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

from re import error
import sys
from dataclasses import dataclass
from typing import Optional, List

### Constants ##################################################################
PROGRAM = "pubsubserver"


### Data Classes $##############################################################
@dataclass()
class Server:
    port: str
    server: str = "localhost"


@dataclass()
class ServerProgramArgs:
    server_id: str = ""
    listenOnPort: Optional[str] = None
    servers: Optional[List[Server]] = None
    error: bool = False


### Error Handler ##############################################################
class Errors:
    USAGE_ERROR_CODE = 1
    BAD_SERVER_ID = 2

    @staticmethod
    def usage_msg() -> str:
        return f"Usage: {PROGRAM} [--server [server]:port]..." \
                "[--listenon port] serverid"

    @staticmethod
    def bad_server_id_msg(server_id) -> str:
        return f"{PROGRAM}: bad server ID \"{server_id}\""

    @staticmethod
    def unknown_error_msg() -> str:
            return f"{PROGRAM}: Unknown Error Detected"


### Functions ##################################################################

def print_stderr(message: str) -> None:
    """Helper method for printing a message to stderr."""
    print(message, file=sys.stderr)


def print_stdout(message: str) -> None:
    """Helper method for printing a message to stdout."""
    print(message, file=sys.stdout)


def show_error(error_code: int, **kwargs) -> None:
    """Given an error code, print the matching message."""
    match error_code:
        case Errors.USAGE_ERROR_CODE:
            print_stderr(Errors.usage_msg())
        case _:
            print_stderr(Errors.unknown_error_msg())


def exit_program(error_code: int) -> None:
    """Exit from program with given error_code."""
    print_stderr(f"\n--DEBUG--\nExited with code '{error_code}'")
    sys.exit(error_code)


def parse_arguments(arguments: list[str]) -> ServerProgramArgs:
    """ """
    program_args: ServerProgramArgs = ServerProgramArgs()

    return program_args


### MAIN #######################################################################
def main():
    
    ## Command line argument parsing
    arguments: ServerProgramArgs = parse_arguments(sys.argv[1:])
    if arguments.error:
        show_error(Errors.USAGE_ERROR_CODE)
        exit_program(Errors.USAGE_ERROR_CODE)


if __name__ == "__main__":
    main()
















