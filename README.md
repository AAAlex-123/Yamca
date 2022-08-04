# Messaging App

## Building the project

Execute `build.bat` or compile every `.java` file in the `src` directory

## Running the Servers

#### Run the "default" Server, where other server nodes will connect to:
Execute `run_server.bat` with no arguments or run `java --class-path bin app.Server`

#### Run a non-default Server that connects to the default Server node:
Run `run_server.bat <ip> <broker_port>` or run `java --class-path bin app.Server <ip>
<broker_port>`, where:
- `ip` and `broker_port` will be printed to standard out when running the default Server

## Running the Clients

#### Run a Client for a new Profile that connects to the default Server node:
Run `run_client.bat -c <name> <ip> <port> <user_dir>` or run `java --class-path bin app.Client -c
<name> <ip> <port> <user_dir>`, where:
-  `name` is the name of the new Profile
- `ip` and `broker_port` will be printed to standard out when running the default Server
- `user_dir` is the directory where the new Profile will be stored

#### Run a Client for an existing Profile that connects to the default Server node:
Run `run_client.bat -l <id> <ip> <port> <user_dir>` or run `java --class-path bin app.Client -l <id>
<ip> <port> <user_dir>`, where:
- `id` is the id of the existing Profile (the name of the directory that corresponds to that 
Profile)
- `ip` and `broker_port` will be printed to standard out when running the default Server
- `user_dir` is the directory where the new Profile will be stored
