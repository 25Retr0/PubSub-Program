# PubSub Program

Client + Server program for a network proramming assignment in UQ's Computer Networking 1 (COMS3200) Course

## Description
- Two programs (pubsubserver and pubsubclient) that implement a publish-subscribe system.
- Built on top of the TCP transport layer using Python's socket library
- Custom Application Layer communication protocol between Client and Server

## Functionality

### Client:
The pubsubclient program provides a command line interface for subscribing to particular topics (and therefore receiving messages that match those topics) and for publishing messages on particular topics to a connected pubsubserver.

#### How to run pubsubclient
```bash
python3 pubsubclient.py [--topic topic] [server]:port clientid [message]
```

Meaning of each argument:
 - `--topic` - specifies that the following value argument is the initial default topic for messages published by this client
 - `[server]:port` - argument must be present and optionally specifies the name or IPv4 address of the server to connect to before the colon (localhost is used if not specified) and, after the colon, the port number of service name to connect to
 - `clientid` - argument must be present and is a unique identifier for the client
 - `message` - optional argument that indicates that the client should immediately publish the given message. *REQUIRES* the `--topic` argument to be given with an associated topic value

 #### Functions of pubsubclient

 *TODO*


### Server:

 *TODO*
