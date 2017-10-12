"""A python plugin-based cluster management system."""
import cmd
import struct
import json
import tempfile
import os
import shutil
import shlex
import argparse
import sys
import platform
import socket
import pprint
import time

try:
    import multiprocessing
except ImportError:
    multiprocessing = None

from twisted.internet import task, threads, protocol, defer, endpoints, utils, reactor
from twisted.internet.error import ProcessDone, ConnectionDone
from twisted.python.log import startLogging
from twisted.python.util import println
from twisted.protocols.basic import IntNStringReceiver


# ================= CONSTANTS ======================

NAME = "pypbcms"
VERSION = "0.0.1"

DEFAULT_PORT = 6925

LENGTH_PREFIX_FORMAT = "!Q"
LENGTH_PREFIX_LENGTH = struct.calcsize(LENGTH_PREFIX_FORMAT)
MAX_MESSAGE_LENGTH = struct.unpack(LENGTH_PREFIX_FORMAT, "\xff" * LENGTH_PREFIX_LENGTH)[0]

TEMP_DIR = os.path.join(tempfile.gettempdir(), NAME)





# ================= UTILITIES ======================

class ProcessControlProtocol(protocol.ProcessProtocol):
    """A protocol used to implement the do_shell() method of the ManagementShell."""
    def __init__(self, d):
        self.d = d

    def processEnded(self, status):
        """called when the process ended."""
        v = status.value
        if isinstance(v, ProcessDone):
            self.d.callback(0)
        else:
            self.d.callback(v.exitCode)


def is_None(obj):
    """returns True if object is None."""
    return (obj is None)


def is_not_None(obj):
    """returns True if object is not None."""
    return (obj is not None)


# ================= PROTOCOLS ======================

class JsonProtocol(IntNStringReceiver):
    """
    A protocol for exchanging JSON messages.
    It also contains a mechanic for setting another target for the received data.
    """
    structFormat = LENGTH_PREFIX_FORMAT
    prefixLength = LENGTH_PREFIX_LENGTH
    MAX_LENGTH = MAX_MESSAGE_LENGTH

    def __init__(self):
        self.receiver = None

    def stringReceived(self, s):
        """called when a string was received"""
        if self.receiver is not None:
            self.receiver.dataReceived(s)
        else:
            try:
                content = json.loads(s)
            except:
                pass
            else:
                return self.messageReceived(content)

    def messageReceived(self, msg):
        """called when a json message was received."""
        pass

    def set_receiver(self, r):
        """sets the protocol into raw mode and relay the data to the specified receiver."""
        self.receiver = r

    def remove_receiver(self):
        """removes the receiver and sets the protocol into json mode."""
        self.receiver = None

    def send_message(self, msg):
        """encodes the message as json and sends it to the peer."""
        s = json.dumps(msg)
        self.sendString(s)

    def send_raw(self, s):
        """sends a raw string to the peer."""
        self.sendMessage(s)


class ServerAndClientSharedProtocol(JsonProtocol):
    """The part of the protocol shared by both the server and the client."""
    DATA_DIR = TEMP_DIR

    MSG_TYPE_ACTION = "ACTION"

    def __init__(self):
        JsonProtocol.__init__(self)
        self.mid2plugin = {}

    def is_path_allowed(self, p):
        """returns True if the path p is allowed to be accessed, False otherwise."""
        lap = os.path.abspath(self.DATA_DIR)
        ap = os.path.abspath(os.path.join(lap, p))
        return ap.startswith(lap)

    def disconnect(self):
        """disconnects the protocol."""
        self.transport.loseConnection()

    def messageReceived(self, msg):
        """called when a message was received."""
        t = msg.get("type", self.MSG_TYPE_ACTION)
        if t == self.MSG_TYPE_ACTION:
            n = msg.get("action_name", None)
            if (n is None) or ("." in n):
                return
            fn = "handle_" + n
            if not hasattr(self, fn):
                return
            f = getattr(self, fn)
            a = msg.get("data", None)
            return f(a)
        else:
            return None

    def call_action(self, name, data):
        """calls an action on the peer providing data as an argument."""
        msg = {
            "type": self.MSG_TYPE_ACTION,
            "action_name": name,
            "data": data,
            }
        self.send_message(msg)

    def handle_init_file_transfer(self, msg):
        """handles an incomming file transfer."""
        p = os.path.join(self.DATA_DIR, msg.get("dest", "unnamed_file"))
        l = msg.get("length", None)
        if not self.is_path_allowed(p):
            self.disconnect()
        if (l is None) or (not isinstance(l, (int, long))):
            self.disconnect()
        mrp = MessageReceiverProtocol(self, p, l)
        mrp.start()

    def send_file(self, name, content):
        """sends a file to the peer."""
        l = len(content)
        data = {"dest": name, "length": l}
        self.call_action(self, "init_file_transfer", data)
        self.send_raw(content)

    def send_version(self):
        """sends the version to the server."""
        self.call_action("version", {"version": VERSION})


class ServerProtocol(ServerAndClientSharedProtocol):
    """The protocol for the server"""
    def __init__(self, factory, cid):
        ServerAndClientSharedProtocol.__init__(self)
        self.factory = factory
        self.info = {}
        self.cid = cid
        self.working = False

    def connectionMade(self):
        """called when the connection was made."""
        self.factory.add_client(self)

    def connectionLost(self, reason):
        """called when the connection was lost."""
        self.factory.remove_client(self)

    def handle_version(self, msg):
        """handles the version message."""
        v = msg.get("version", None)
        self.send_version()
        if v != VERSION:
            reactor.callLater(1.0, self.disconnect)

    def handle_set_info(self, info):
        """sets the local system info to the received system info."""
        self.info = info

    def handle_set_ready(self, msg):
        """sets the ready state."""
        ns = msg.get("working", False)
        self.working = ns
        self.info["working"] = self.working

    def run_command(self, command):
        """runs the command on the client."""
        self.call_action("shell", {"command": command})


class ClientProtocol(ServerAndClientSharedProtocol):
    """The protocol for the client."""
    def __init__(self, ns, d):
        ServerAndClientSharedProtocol.__init__(self)
        self.ns = ns
        self.d = d

    def connectionMade(self):
        """called when the connection was established."""
        self.send_version()

    def connectionLost(self, reason):
        """called when the connection was lost."""
        if isinstance(reason.value, ConnectionDone):
            self.d.callback(None)
        else:
            self.d.errback(reason)

    def handle_version(self, msg):
        """handles the version response from the server."""
        v = msg.get("version", None)
        if v != VERSION:
            println("Error: Version mismatch! Client version: {c} Server version: {s}.".format(c=VERSION, s=v))
            self.disconnect()
        else:
            self.send_info()
            self.set_working(False)

    @defer.inlineCallbacks
    def handle_shell(self, msg):
        """handles a shell command."""
        command = msg.get("command", None)
        if command is None:
            pass
        else:
            self.set_working(False)
            v = yield utils.getProcessValue(command[0], command)
            self.set_working(True)

    def handle_sendinfo(self, msg):
        """sends our system information *again*."""
        self.send_info()
        self.set_working()

    def send_info(self):
        """sends platform information to the server."""
        data = {
            "hostname": socket.gethostname(),
            "machine": platform.machine(),
            "node": platform.node(),
            "platform": platform.platform(),
            "processor": platform.processor(),
            "python_implementation": platform.python_implementation(),
            "system": platform.system(),
            "release": platform.release(),
            "pid": os.getpid(),
            }
        self.call_action("set_info", data)

    def set_working(self, state):
        """tells the server that this client is (not) working."""
        self.call_action("set_working", {"working": state})


class MessageReceiverProtocol(protocol.Protocol):
    """A protocol for receiving a file."""
    def __init__(self, master, path, length):
        self.master = master
        self.path = path
        self.length = length
        self.received = 0
        self.f = None
        self.started = False

    def start(self):
        """starts receiving the data and writes it to the file."""
        if self.started:
            raise RuntimeError("{s} already started!".format(s=self))
        if os.path.exists(self.path):
            if os.path.isdir(self.path):
                shutil.rmtree(self.path)
            else:
                os.remove(self.path)
        self.f = open(self.path, "wb")
        self.received = 0
        self.started = True
        self.master.set_receiver(self)

    def stop(self):
        """stops receiving data, closes the file and removes the protocol."""
        self.started = False
        self.master.remove_receiver()
        if self.f is not None:
            self.f.close()

    def dataReceived(self, s):
        """called when some data was received."""
        if not self.started:
            raise RuntimeError("{s} not started but already receiving data!".format(s=self))
        self.f.write(s)
        l = len(s)
        self.received += l
        if (self.length - l) <= 0:
            self.stop()



class ServerFactory(protocol.Factory):
    """The protocol factory for the server."""

    def __init__(self):
        self.clients = {}
        self.cur_cid = 0

    def buildProtocol(self, addr):
        """builds a protocol for the communication with the client and returns it."""
        p = ServerProtocol(self, cid=self.cur_cid)
        self.cur_cid += 1
        return p

    def add_client(self, p):
        """adds a client to the internal client list."""
        peer = p.cid
        self.clients[peer] = p

    def remove_client(self, p):
        """removes a client from the internal client list."""
        peer = p.cid
        del self.clients[peer]

    def list_client_ids(self):
        """returns a list containting the client ids of all currently connected clients."""
        return self.clients.keys()

    def get_client(self, cid):
        """returns the client with the given client id or None if it is not found."""
        return self.clients.get(cid, None)

    def get_clients(self, cids, include_None=True):
        """returns a list containing the clients with the given client ids."""
        ret = []
        for c in cids:
            p = self.get_client(c)
            if (p is None) and (not include_None):
                continue
            ret.append(p)
        return ret

    def get_working_client_ids(self):
        """returns a list containing the client ids of all the working clients."""
        t = filter(is_not_None, [e if self.get_client(e).working else None for e in self.list_client_ids()])
        return t

    def search_for_clients(self, conditions, include_empty=False):
        """
        Returns a list of all client ids of the clients matching the conditions.
        'conditions' should be a dictionar specifying the key: values to check for matches in the client information.
        A client is considered matching the conditions when all values of the keys defined in 'conditions' match with
        the values of the same keys in the client information.
        If 'include_empty' (default: False), empty client information is considered a match.
        """
        matches = []
        cids = self.list_client_ids()
        for cid in cids:
            p = self.get_client(cid)
            if p is None:
                continue
            if p.info == {}:
                if (p.info == conditions) or include_empty:
                    matches.append(cid)
                else:
                    continue
            else:
                dm = True
                for k in conditions:
                    if not k in p.info:
                        dm = False
                        break
                    v1 = conditions[k]
                    v2 = p.info[k]
                    if v1 != v2:
                        dm = False
                        break
                if dm:
                    matches.append(cid)
        return matches



# ================= SHELL ======================

class ManagementShell(cmd.Cmd):
    """The management shell."""
    intro = "{n} v{v}".format(n=NAME, v=VERSION)

    def __init__(self, factory):
        cmd.Cmd.__init__(self)
        # self.prompt = "({n})".format(n=NAME)
        self.factory = factory
        self.selected = []
        self.update_prompt()

    def write(self, msg):
        """writes a message."""
        self.stdout.write(msg)

    def pprint(self, obj):
        """pretty prints object."""
        s = pprint.pformat(obj)
        if not s.endswith("\n"):
            s += "\n"
        self.write(s)

    def filter_selected(self):
        """removes all invalid client selections from the selection, including disconnected clients."""
        # 1. remove all invalid (non-int) values
        self.selected = filter(is_not_None, [e if isinstance(e, (int, long)) else None for e in self.selected])
        # 2. remove all disconnected/non-existent clients
        self.selected = filter(is_not_None, [e if (self.factory.get_client(e) is not None) else None for e in self.selected])

    def update_prompt(self):
        """updates the prompt."""
        n_selected = len(self.selected)
        n_working = len(self.factory.get_working_client_ids())
        if n_working > 0:
            ws = "|{n} working".format(n=n_working)
        else:
            ws = ""
        self.prompt = "({n} selected{ws})".format(n=n_selected, ws=ws)

    def precmd(self, line):
        """called before a command executes."""
        self.filter_selected()
        self.update_prompt()
        return line

    def postcmd(self, stop, line):
        """called when a command finished."""
        self.filter_selected()
        self.update_prompt()
        return stop

    def do_EOF(self, l):
        """EOF|quit|exit|q: exits the shell and stops the server."""
        return True

    do_exit = do_quit = do_q = do_EOF

    def do_shell(self, l):
        """shell <cmd>: executes cmd in a local shell."""
        r = threads.blockingCallFromThread(reactor, self._do_shell, l)
        self.write("Done. Exit code: {r}.\n".format(r=r))

    def _do_shell(self, l):
        """executes the shell line in the reactor thread."""
        splitted = shlex.split(l)
        d = defer.Deferred()
        p = ProcessControlProtocol(d)
        reactor.spawnProcess(
            p,
            splitted[0],
            splitted,
            # usePTY=True,
            childFDs={0:0, 1:1, 2:2},
            )
        return d

    def do_select(self, l):
        """select <ALL|NONE|condition> [-e]: sets the selected clients."""
        args = shlex.split(l)
        include_empty = False
        if len(args) == 1:
            cs = args[0]
            if cs == "ALL":
                self.selected = self.factory.list_client_ids()
                self.update_prompt()
                return
            elif cs == "NONE":
                self.selected = []
                self.update_prompt()
                return
        elif len(args) == 2:
            if "-e" not in args:
                self.write("Usage Error: unknown argument.\n")
                return
            elif "-e" in args:
                include_empty = True
                args.remove("-e")
                cs = args[0]
            else:
                raise RuntimeError("Logic Error!")
        else:
            self.write("Usage error. Usage: select [-e] <condition|ALL|NONE>\n")
        try:
            c = eval(cs)
        except Exception as e:
            self.write("Error evaluating selection conditions:\n")
            self.write(repr(e) + "\n")
            return
        if isinstance(c, (int, long)):
            self.selected = [c]
        elif isinstance(c, (list, tuple)):
            self.selected = []
            for e in c:
                if not isinstance(e, (int, long)):
                    self.write("WARNING: invalid list/tuple element '{e}' ignored!\n".format(e=e))
                else:
                    self.selected.append(e)
            self.selected = c
            self.filter_selected()
            self.update_prompt()
        self.selected = self.factory.search_for_clients(c, include_empty=include_empty)
        self.update_prompt()

    def do_selected(self, l):
        """selected: prints the selected ids."""
        self.pprint(self.selected)

    def do_show(self, l):
        """show <cid>: shows the client info."""
        try:
            c = int(l)
        except ValueError:
            self.write("Error: Invalid argument!\n")
            return
        p = self.factory.get_client(c)
        if c is None:
            self.write("Error: No such client!\n")
        self.pprint(p.info)

    def do_disconnect(self, l):
        """disconnect: disconnects all selected clients."""
        for c in self.selected:
            p = self.factory.get_client(c)
            p.disconnect()
        time.sleep(0.5)

    def do_remotecommand(self, l):
        """remotecommand [cmd]: executes cmd on the remote servers."""
        rc = shlex.split(l)
        clients = self.factory.get_clients(self.selected, include_None=False)
        for c in clients:
            c.run_command(rc)



# ================= MAIN CODE ======================

def start_shell(reactor, ns):
    """launches a shell and returns a deferred, which will be fired when the shell is closed."""
    factory = ServerFactory()
    ep = endpoints.TCP4ServerEndpoint(reactor, interface="0.0.0.0", port=ns.port)
    ep.listen(factory)
    if ns.shell:
        shell = ManagementShell(factory)
        d = threads.deferToThread(shell.cmdloop)
        return d
    else:
        d = defer.Deferred()
        return d

def start_client(reactor, ns):
    """starts the client and connects to the server."""
    d = defer.Deferred()
    p = ClientProtocol(ns, d)
    ep = endpoints.TCP4ClientEndpoint(reactor, host=ns.host, port=ns.port)
    cpd = endpoints.connectProtocol(ep, p)
    cpd.addErrback(d.errback)
    return d


def main():
    """
    The main function.
    We start the server and a shell in a thread and wait for its completion.
    """
    parser = argparse.ArgumentParser(description="A python plugin-based cluster management system")
    parser.add_argument("action", action="store", choices=["server", "client"], help="what to do")
    parser.add_argument("-v", "--verbose", action="store_true", help="print additional information")
    parser.add_argument("-H", "--host", action="store", help="host/interface to connect/bind to", default="0.0.0.0")
    parser.add_argument("-p", "--port", action="store", type=int, default=DEFAULT_PORT, help="port of the server")
    parser.add_argument("--noshell", action="store_false", dest="shell", help="do not start a shell when starting the server")
    ns = parser.parse_args()
    if ns.verbose:
        startLogging(sys.stdout)
    if ns.action == "server":
        f = start_shell
    elif ns.action == "client":
        f = start_client
    task.react(f, (ns, ))


if __name__ == "__main__":
    main()
