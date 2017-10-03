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

from twisted.internet import task, threads, protocol, defer, endpoints, reactor
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
            if not hasattr(self, n):
                return
            f = getattr(self, "handle_" + n)
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
    def __init__(self, factory):
        ServerAndClientSharedProtocol.__init__(self)
        self.factory = factory

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

    def buildProtocol(self, addr):
        """builds a protocol for the communication with the client and returns it."""
        p = ServerProtocol(self)
        return p

    def add_client(self, p):
        """adds a client to the internal client list."""
        peer = p.transport.getPeer()
        self.clients[peer] = p

    def remove_client(self, p):
        """removes a client from the internal client list."""
        peer = p.transport.getPeer()
        del self.clients[peer]



# ================= SHELL ======================

class ManagementShell(cmd.Cmd):
    """The management shell."""
    prompt = "({n})".format(n=NAME)
    intro = "{n} v{v}".format(n=NAME, v=VERSION)

    def __init__(self, factory):
        cmd.Cmd.__init__(self)
        self.factory = factory

    def write(self, msg):
        """writes a message."""
        self.stdout.write(msg)

    def do_EOF(self, l):
        """exits the script."""
        return True

    do_exit = do_quit = do_q = do_EOF

    def do_shell(self, l):
        """processes a shell command."""
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
