# pypbcms
A plugin based cluster management system written in python.

# Features
- plugin based - easily extendable
- builtin plugin syncing
- select specific nodes to execute actions on
  - select by node id
  - select by tags (and add/remove tags(
  - select by conditions (e.g. python version, number of cores, hostname...)
- some builtin commands. Examples:
  - run a command on each node or each core
  - transfer a directorie and execute a command on each node or each core
- client auto-reconnect (option `-r`)
- auto-detection of server/master/primary node (options `-b`, `-i` and `-a`)
- single file (excluding optional plugins)
- Command-Shell
- pure python / no C-dependencies

# Installation
**Using pip:**
1. `pip install pypbcms`.

**Using git and `setup.py`:**
1. `git clone git@github.com:bennr01/pypbcms.git`
2. `cd pypbcms`
3. `python setup.py install`

**Without installation:**
1. Make sure you have `python`, `Twisted` and `zope.interface` installed.
2. Download [pypbcms.py](https://raw.githubusercontent.com/bennr01/pypbcms/master/pypbcms.py)

# Usage
**If installed using pip or `setup.py`:**
See `pypbcms --help`

**If not installed:**
`python /path/to/pypbcms.py --help`

*pypbcms* distinguishes between the server and the clients.
The server
 - is the node all other nodes connect to
 - controlls the other nodes
 - launches the management shell
 - does not need to be the same master/primary node other programs use as their master/primary node. It does not tell your program which master node it should use.
 - is the node from which the plugins will be synced
 - does not automatically launch a client on the same node.
 - can broadcast its ip/port using UDP (option `-b`)
 The clients:
 - are the nodes on which commands will be run
 - do not offer a user interface
 - can automatically connect to the server (option `-a`)
 
 
