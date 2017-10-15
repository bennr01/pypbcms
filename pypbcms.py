"""A python plugin-based cluster management system."""
import cmd
import struct
import json
import tempfile
import os
import shlex
import argparse
import sys
import platform
import socket
import pprint
import time
import functools
import collections

try:
    import multiprocessing
except ImportError:
    multiprocessing = None

from twisted.internet import task, threads, protocol, defer, endpoints, utils, reactor
from twisted.internet.error import ProcessDone, ConnectionDone
from twisted.python.log import startLogging
from twisted.python.threadable import isInIOThread
from twisted.protocols.basic import IntNStringReceiver


# ================= CONSTANTS ======================

NAME = "pypbcms"
VERSION = "0.0.1"

DEFAULT_PORT = 6925

LENGTH_PREFIX_FORMAT = "!Q"
LENGTH_PREFIX_LENGTH = struct.calcsize(LENGTH_PREFIX_FORMAT)
MAX_MESSAGE_LENGTH = struct.unpack(LENGTH_PREFIX_FORMAT, "\xff" * LENGTH_PREFIX_LENGTH)[0]

TEMP_DIR = os.path.join(tempfile.gettempdir(), NAME)


# ================= EXCEPTIONS ======================

class RPCError(Exception):
    """An Error occured during a RPC procedure."""
    pass



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


def command_on_reactor_loop(f):
    """
    A decorator for cmd.Cmd().do_* functions which have to be called on the main reactor loop.
    """
    @functools.wraps(f)
    def wrapper(*args, **kwargs):
        if not isInIOThread():
            return threads.blockingCallFromThread(reactor, f, *args, **kwargs)
        else:
            return f(*args, **kwargs)
    return wrapper


# ================= PROTOCOLS ======================

class DualRPCProtocol(IntNStringReceiver):
    """
    A RPC protocol which allows both sides to make RPC calls.
    """
    structFormat = LENGTH_PREFIX_FORMAT
    prefixLength = LENGTH_PREFIX_LENGTH
    MAX_LENGTH = MAX_MESSAGE_LENGTH

    RPC_PREFIX = "remote_"  # prefix to identify rpc functions

    # constants to identify message types
    _TYPE_VERSION_CHECK = "version_check"
    _TYPE_VERSION_RESPONSE = "version_response"
    _TYPE_RPC_REQUEST = "rpc_request"
    _TYPE_RPC_RESPONSE = "rpc_response"

    # constants to identify response success
    _STATUS_OK = "OK"
    _STATUS_ERROR = "error"

    def __init__(self):
        self._cur_id = 0
        self._id2d = {}  # id -> Deferred()

    def _get_id(self):
        """returns a new id used to identify a rpc procedure."""
        ret, self._cur_id = self._cur_id, self._cur_id + 1
        return ret

    def connectionMade(self):
        """called when the connection was established."""
        self._did_version_check = False
        self._send_version_check()

    def connectionLost(self, reason):
        """called when the connection was lost."""
        for cid in self._id2d:
            d = self._id2d[cid]
            d.errback(
                RPCError(
                    "The connection lost before a response was received."
                    )
                )

    def _send_version_check(self):
        """sends the version check message"""
        self.send_message(
            {
                "type": self._TYPE_VERSION_CHECK,
                "version": VERSION,
                }
            )

    @defer.inlineCallbacks
    def stringReceived(self, s):
        """called when a string was received"""
        try:
            content = json.loads(s)
        except Exception:
            self.loseConnection()
            defer.returnValue(None)
        
        mtype = content.get("type", None)
        if mtype == self._TYPE_VERSION_CHECK:
            ov = content.get("version", None)
            match = (ov == VERSION)
            self.send_message(
                {
                    "type": self._TYPE_VERSION_RESPONSE,
                    "version": VERSION,
                    "match": match,
                    }
                )
            if not match:
                self.loseConnection()
                self._did_version_check = False  # set to false to ignore incomming messages
            else:
                self._did_version_check = True
            defer.returnValue(None)
        elif mtype == self._TYPE_VERSION_RESPONSE:
            m = content.get("match", False)
            if not m:
                self.loseConnection()
                self._did_version_check = False
            else:
                self._did_version_check = True
            defer.returnValue(None)
        elif not self._did_version_check:
            # ignore message
            # this elif will block the execution of the following 'elif' statements.
            defer.returnValue(None)
        elif mtype in (self._TYPE_RPC_REQUEST, self._TYPE_RPC_RESPONSE):
            cid = content.get("id", None)
            if cid is None:
                self.loseConnection()
                defer.returnValue(None)
            if mtype == self._TYPE_RPC_REQUEST:
                fn = content.get("name", None)
                args = tuple(content.get("args", ()))
                kwargs = content.get("kwargs", {})
                ffn = self.RPC_PREFIX + fn
                if (fn is None) or ("." in fn):
                    defer.returnValue(None)
                if not hasattr(self, ffn):
                    answer = {
                        "type": self._TYPE_RPC_RESPONSE,
                        "id": cid,
                        "status": self._STATUS_ERROR,
                        "error_message": repr(
                            KeyError(
                                "No such method or function: '{n}'".format(
                                    n=fn,
                                    )
                                )
                            ),
                        }
                    self.send_message(answer)
                    defer.returnValue(None)
                f = getattr(self, ffn)
                try:
                    res = yield f(*args, **kwargs)
                except Exception as e:
                    answer = {
                        "type": self._TYPE_RPC_RESPONSE,
                        "id": cid,
                        "status": self._STATUS_ERROR,
                        "error_message": repr(e),
                        }
                else:
                    answer = {
                        "type": self._TYPE_RPC_RESPONSE,
                        "id": cid,
                        "status": self._STATUS_OK,
                        "result": res,
                        }
                self.send_message(answer)
            elif mtype == self._TYPE_RPC_RESPONSE:
                d = self._id2d[cid]
                del self._id2d[cid]
                did_error = (content.get("status", self._STATUS_ERROR) == self._STATUS_ERROR)
                if did_error:
                    msg = content.get("error_message", "Unknown RPC Error")
                    error = RPCError(msg)
                    d.errback(error)
                else:
                    res = content.get("result", None)
                    d.callback(res)
        else:
            # protocol violation
            self.loseConnection()

    def send_message(self, msg):
        """encodes the message as json and sends it to the peer."""
        s = json.dumps(msg)
        self.sendString(s)

    def call_remote(self, fname, *args, **kwargs):
        """
        Calls e remote function.
        Returns a deferred which will either fire with the result or errback with the error message.
        """
        cid = self._get_id()
        d = defer.Deferred()
        self._id2d[cid] = d
        self.send_message(
            {
                "type": self._TYPE_RPC_REQUEST,
                "id": cid,
                "name": fname,
                "args": args,
                "kwargs": kwargs,
                }
            )
        return d


class ServerAndClientSharedProtocol(DualRPCProtocol):
    """The part of the protocol shared by both the server and the client."""
    DATA_DIR = TEMP_DIR

    def is_path_allowed(self, p):
        """returns True if the path p is allowed to be accessed, False otherwise."""
        lap = os.path.abspath(self.DATA_DIR)
        ap = os.path.abspath(os.path.join(lap, p))
        return ap.startswith(lap)

    def disconnect(self):
        """disconnects the protocol."""
        self.transport.loseConnection()

    def remote_set_file_content(self, name, content):
        """sets the content of the file."""
        if os.path.isabs(name):
            p = name
        else:
            p = os.path.join(self.DATA_DIR, name)
        if not self.is_path_allowed(p):
            raise IOError("Path not allowed!")
        with open(p, "wb") as fout:
            fout.write(content)

    def send_file(self, name, content):
        """sends a file to the peer."""
        self.call_remote("set_file_content", name, content)

    def send_version(self):
        """sends the version to the server."""
        self.call_action("version", {"version": VERSION})


class ServerProtocol(ServerAndClientSharedProtocol):
    """The protocol for the server"""
    def __init__(self, factory, cid):
        ServerAndClientSharedProtocol.__init__(self)
        self.factory = factory
        self.cid = cid
        self.working = False

    def connectionMade(self):
        """called when the connection was made."""
        self.factory.add_client(self)

    def connectionLost(self, reason):
        """called when the connection was lost."""
        self.factory.remove_client(self)

    def run_command(self, command):
        """runs the command on the client."""
        return self.call_remote("shell", command=command)

    def get_info(self):
        """returns client information."""
        return self.call_remote("get_info")

    def is_working(self):
        """returns a deferred firing with a bool indicating wether the client is working or not."""
        return self.call_remote("is_working")

    def set_tag(self, key, value):
        """
        Sets a key in the info to the specified value.
        This can be used to 'tag' clients in order to create groups.
        A tag can replace a normal key/value-pair in the info.
        """
        return self.call_remote("set_tag", key, value)

    def remove_tag(self, key):
        """removes a tag."""
        return self.call_remote("remove_tag", key)


class ClientProtocol(ServerAndClientSharedProtocol):
    """The protocol for the client."""
    def __init__(self, ns, d):
        ServerAndClientSharedProtocol.__init__(self)
        self.ns = ns
        self.d = d
        self.is_working = False
        self.tags = {}

    def set_working(self, status=True):
        """sets the working status of the client."""
        self.is_working = status

    def connectionLost(self, reason):
        """called when the connection was lost."""
        ServerAndClientSharedProtocol.connectionLost(self, reason)
        if isinstance(reason.value, ConnectionDone):
            self.d.callback(None)
        else:
            self.d.errback(reason)

    @defer.inlineCallbacks
    def remote_shell(self, command):
        """executes a shell command."""
        self.set_working(True)
        v = yield utils.getProcessValue(command[0], command[1:])
        self.set_working(False)
        defer.returnValue(v)

    def remote_get_info(self):
        """returns platform information of the client."""
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
        data.update(self.tags)
        return data

    def remote_is_working(self):
        """returns a boolean indicating wether the client is currently working or not."""
        return self.is_working

    def remote_set_tag(self, key, value):
        """sets a tag to the specified value."""
        self.tags[key] = value

    def remote_remove_tag(self, key):
        """removes a tag."""
        if key in self.tags:
            del self.tags[key]


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

    @defer.inlineCallbacks
    def get_working_client_ids(self):
        """returns a defered firing with a list containing the client ids of all working clients."""
        ret = []
        cids = self.list_client_ids()
        for cid in cids:
            p = self.get_client(cid)
            working = yield p.is_working()
            if working:
                ret.append(cid)
        defer.returnValue(ret)

    @defer.inlineCallbacks
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
            pinfo = yield p.get_info()
            if pinfo == {}:
                if (pinfo == conditions) or include_empty:
                    matches.append(cid)
                else:
                    continue
            else:
                dm = True
                for k in conditions:
                    if not k in pinfo:
                        dm = False
                        break
                    v1 = conditions[k]
                    v2 = pinfo[k]
                    if v1 != v2:
                        dm = False
                        break
                if dm:
                    matches.append(cid)
        defer.returnValue(matches)



# ================= SHELL ======================

class ManagementShell(cmd.Cmd):
    """The management shell."""
    intro = "{n} v{v}".format(n=NAME, v=VERSION)

    def __init__(self, factory):
        cmd.Cmd.__init__(self)
        self.factory = factory
        self.selected = []
        self.prompt = "(0 selected)"
        # self.update_prompt()

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

    @command_on_reactor_loop
    @defer.inlineCallbacks
    def update_prompt(self, ig=None):
        """updates the prompt."""
        n_selected = len(self.selected)
        wcids = yield self.factory.get_working_client_ids()
        n_working = len(wcids)
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

    @command_on_reactor_loop
    @defer.inlineCallbacks
    def do_select(self, l):
        """select <ALL|NONE|condition> [-e]: sets the selected clients."""
        args = shlex.split(l)
        include_empty = False
        if len(args) == 1:
            cs = args[0]
            if cs == "ALL":
                self.selected = self.factory.list_client_ids()
                self.update_prompt()
                defer.returnValue(None)
            elif cs == "NONE":
                self.selected = []
                self.update_prompt()
                defer.returnValue(None)
        elif len(args) == 2:
            if "-e" not in args:
                self.write("Usage Error: unknown argument.\n")
                defer.returnValue(None)
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
            defer.returnValue(None)
        if isinstance(c, (int, long)):
            self.selected = [c]
            self.update_prompt()
            defer.returnValue(None)
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
        self.selected = yield self.factory.search_for_clients(c, include_empty=include_empty)
        self.update_prompt()

    def do_selected(self, l):
        """selected: prints the selected ids."""
        self.pprint(self.selected)

    @command_on_reactor_loop
    @defer.inlineCallbacks
    def do_show(self, l):
        """show <cid>: shows the client info."""
        try:
            c = int(l)
        except ValueError:
            self.write("Error: Invalid argument!\n")
            defer.returnValue(None)
        p = self.factory.get_client(c)
        if p is None:
            self.write("Error: No such client: '{c}'!\n".format(c=c))
            defer.returnValue(None)
        pinfo = yield p.get_info()
        self.pprint(pinfo)

    @command_on_reactor_loop
    def do_disconnect(self, l):
        """disconnect: disconnects all selected clients."""
        for c in self.selected:
            p = self.factory.get_client(c)
            p.disconnect()
        time.sleep(0.5)

    @command_on_reactor_loop
    @defer.inlineCallbacks
    def do_remotecommand(self, l):
        """remotecommand [cmd]: executes cmd on the remote servers."""
        rc = shlex.split(l)
        ds = []
        clients = self.factory.get_clients(self.selected, include_None=False)
        for c in clients:
            d = c.run_command(rc)
            ds.append(d)
        if len(ds) > 0:
            codes = yield defer.gatherResults(ds)
            counter = collections.Counter(codes)
            self.write("Done. Exit codes:\n")
            self.pprint(dict(counter))
        else:
            self.write("Error: No clients found; no commands executed.\n")

    @command_on_reactor_loop
    @defer.inlineCallbacks
    def do_set_tag(self, l):
        """set_tag <name> <value>: sets a tag for the selected clients."""
        splitted = shlex.split(l)
        if len(splitted) != 2:
            self.write("Usage: set_tag <name> <value>\n")
            defer.returnValue(None)
        key, value = splitted
        ds = []
        clients = self.factory.get_clients(self.selected, include_None=True)
        for c in clients:
            d = c.set_tag(key, value)
            ds.append(d)
        if len(ds) > 0:
            yield defer.gatherResults(ds)
            self.write("Done.\n")
        else:
            self.write("Error: No clients found; no tags set.\n")

    @command_on_reactor_loop
    @defer.inlineCallbacks
    def do_remove_tag(self, l):
        """remove_tag <name>: removes a tag from the selected clients."""
        splitted = shlex.split(l)
        if len(splitted) != 1:
            self.write("Usage: remove_tag <name>\n")
            defer.returnValue(None)
        key = splitted[0]
        ds = []
        clients = self.factory.get_clients(self.selected, include_None=True)
        for c in clients:
            d = c.remove_tag(key)
            ds.append(d)
        if len(ds) > 0:
            yield defer.gatherResults(ds)
            self.write("Done.\n")
        else:
            self.write("Error: No clients found; no tags removed.\n")
        



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
