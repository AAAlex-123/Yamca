# Messaging App

## Building the project

Execute `build.bat` or compile every `.java` file in the `src` directory

## Running the Servers

The servers are persistent, meaning that any data created during the system's runtime will be saved
and restored the next time the servers are run. All such data is stored in the `broker_dir` (passed
as commandline argument when run), which may be different for each server, or the same.

The first server run shall be the "default" server. All subsequent servers shall connect to it. The
connection information of all servers is printed to standard out when they are run.

#### Default Server; other server nodes will connect to this one:
Execute `run_server.bat <broker_dir>`, where:
- `broker_dir` is the directory where all server data will be stored and retrieved from

#### Non-Default Server; connects to the default with the provided connection information:
Run `run_server.bat <broker_dir> <ip> <broker_port>`, where:
- `broker_dir` is the directory where all server data will be stored and retrieved from
- `ip` and `broker_port` is the connection information of the default server

#### Non-Default Server; connects to the default with connection information from file:
Run `run_server.bat <broker_dir> -f <path>`, where:
- `broker_dir` is the directory where all server data will be stored and retrieved from
- `path` is the file that contains the connection information of the default server. The file shall
follow the `.properties` file format and shall contain the `ip` and `port` properties. All other
properties are ignored.

---

Note: ideally, all servers should first be run, followed by any number of clients. Clients may be
initialised and terminated at any point afterwards. Specifically, no servers shall be initialised
after a topic has been created, as the system may misbehave. The servers are persistent, therefore
terminating them and restarting them is not equivalent to a clean boot.

To perform a clean boot, it is necessary to delete all topics, preferably by having the clients
issue such request, since manually deleting the `broker_dir` for all servers does not delete the
local copies of the topics from the clients. Manually deleting the `user_dir` from all clients, in
addition to deleting all `broker_dir`, results in a clean boot.

## Running the Clients

The clients are persistent, meaning that any data created during the client's runtime will be saved
and restored the next time the client is run. All such data is stored in the `user_dir` (passed as
commandline argument when run) which may be either different or the same for each client. Since a
client can run for many different profiles no conflicts will airse.

#### Client that connects to the default Server with the provided connection information:
Run `run_client.bat [-c|-l] <name> <ip> <port> <user_dir>`, where:
- `-c` creates a new profile and `-l` loads an existing profile
- `name` is the name of the profile
- `ip` and `broker_port` is the connection information of the default server
- `user_dir` is the directory where all client data will be stored and retrieved from

#### Client that connects to the default Server with connection information from a file:
Run `run_client.bat [-c|-l] <name> -f <path> <user_dir>`, where:
- `-c` creates a new profile and `-l` loads an existing profile
- `name` is the name of the profile
- `path` is the file that contains the connection information of the default server. The file shall
follow the `.properties` file format and shall contain the `ip` and `port` properties. All other
properties are ignored.
- `user_dir` is the directory where all client data will be stored and retrieved from

