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
@dataclass(frozen=True)
class Constants:
    OPTION_ARGUMENT = "--"

CONSTANTS = Constants()

### Data Classes ###############################################################

@dataclass()
class ClientProgramArgs:
    port: int = -1
    clientid: str = "PLACEHOLDER"
    topic: Optional[str] = None
    server: Optional[str] = None
    message: Optional[str] = None

### Functions ##################################################################

def parse_arguments(arguments: list[str]) -> ClientProgramArgs:
    """ arugments: [--topic topic] [server]:port clientid [message]

    All arguments must happen in this order, if they are specified.
    """
    program_args: ClientProgramArgs = ClientProgramArgs()
    
    return program_args




def run_client():
    pass


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
