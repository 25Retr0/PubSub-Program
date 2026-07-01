import sys

def print_stderr(message: str) -> None:
    """Helper method for printing a message to stderr."""
    print(message, file=sys.stderr)
    sys.stderr.flush()


def print_stdout(message: str) -> None:
    """Helper method for printing a message to stdout."""
    print(message, file=sys.stdout)
    sys.stdout.flush()


def is_valid_topic(topic: str) -> bool:
    """ A valid topic string consists of:
        - Start with a letter (upper or lower)
        - consist of letters, numbers, spaces, and/or '/' (forward slash)
        Returns True if conditions met, otherwise False
    """
    start_letter = topic[0]
    if not start_letter.isalpha():
        return False

    # Check remaining characters follow rules
    for char in topic:
        if char.isalnum() or char in [' ', '/']: continue
        return False
    return True


def is_valid_id(id: str) -> bool:
    """Given an id, returns True if:
        - must be between 2 and 32 characters (inclusive) in length.
        - contain only letters and/or digits
    """
    return ((2 <= len(id) <=32) and id.isalnum());


def is_valid_message(message: str) -> bool:
    """Returns True if the given message is printable. Otherwise False."""
    return message.isprintable()

