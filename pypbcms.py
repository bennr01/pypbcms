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
import functools
import collections
import shutil
import zipfile
import uuid
import base64
import imp

try:
    import multiprocessing
except ImportError:
    multiprocessing = None

from twisted.internet import task, threads, protocol, defer, endpoints, utils, reactor
from twisted.internet.error import ProcessDone, ConnectionDone
from twisted.python.log import startLogging
from twisted.python.threadable import isInIOThread
from twisted.protocols.basic import IntNStringReceiver
from twisted.application.internet import ClientService

from zope.interface import Interface, Attribute


# ================= CONSTANTS ======================

NAME = "pypbcms"
VERSION = "1.1.0"

DEFAULT_PORT = 6925

LENGTH_PREFIX_FORMAT = "!Q"
LENGTH_PREFIX_LENGTH = struct.calcsize(LENGTH_PREFIX_FORMAT)
MAX_MESSAGE_LENGTH = struct.unpack(LENGTH_PREFIX_FORMAT, "\xff" * LENGTH_PREFIX_LENGTH)[0]

_STOP = 1

TEMP_DIR = os.path.join(tempfile.gettempdir(), NAME)  # path of the temporary directory
HOME_DATA_DIR = os.path.join(os.path.expanduser("~"), NAME)
HOME_PLUGIN_DIR = os.path.join(HOME_DATA_DIR, "plugins")
DATA_DIR = TEMP_DIR
WORK_DIR = os.path.join(DATA_DIR, "workspace")  # path of temporary CWD
SYNC_DIR = os.path.join(DATA_DIR, "sync")  # path of the directory where files for the syncinc are kept.
PLUGIN_DIR = os.path.join(TEMP_DIR, "plugins")
PATHS = [TEMP_DIR, HOME_DATA_DIR, HOME_PLUGIN_DIR, DATA_DIR, WORK_DIR, SYNC_DIR, PLUGIN_DIR]
PLUGIN_PATHS = [PLUGIN_DIR, HOME_PLUGIN_DIR]

MAGIC = "{name}/{version}".format(name=NAME, version=VERSION)  # magic to identify broadcast messages
BROADCAST_PORT = 6924


# ================= EXCEPTIONS ======================

class RPCError(Exception):
    """An Error occured during a RPC procedure."""
    pass


class PluginNotFoundError(Exception):
    """The specified plugin was not found."""
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


def chunked(data, chunksize):
    """
    Returns a list of chunks containing at most ``chunksize`` elements of data.
    """
    if chunksize < 1:
        raise ValueError("Chunksize must be at least 1!")
    if int(chunksize) != chunksize:
        raise ValueError("Chunksize needs to be an integer")
    cur = []
    for e in data:
        cur.append(e)
        if len(cur) >= chunksize:
            yield cur
            cur = []
    if cur:
        yield cur

def delete_dir_content(p):
    """deletes the content of the directory"""
    for fn in os.listdir(p):
        fp = os.path.join(p, fn)
        if os.path.isdir(fp):
            shutil.rmtree(fp)
        else:
            os.remove(fp)


def zip_dir(p, outp):
    """writes a directory recusrively into a zipfile"""
    with zipfile.ZipFile(outp, mode="w", allowZip64=True) as zf:
        _write_to_zip(p, "", zf)
        zf.close()
        return


def _write_to_zip(p, fn, zf):
    """writes p to zipfile zf."""
    if os.path.isfile(p):
        zf.write(p, fn)
    else:
        for sfn in os.listdir(p):
            fp = os.path.join(p, sfn)
            san = os.path.join(fn, sfn)
            _write_to_zip(fp, san, zf)

def random_filename():
    """returns a random filename."""
    return uuid.uuid1().hex


def filechunks(f, n=8192):
    """returns a iterator which yields the file content in chunks of n bytes."""
    r = True
    while r:
        data = f.read(n)
        if not data:
            r = False
        yield data


def load_plugins(interface):
    """loads the plugins for 'interface' and returns them."""
    make_global_dirs()
    plugins = []
    plugin_identifiers = []
    # iterate over all plugin install directories
    for pp in PLUGIN_PATHS:
        # iterate over the files and directories inside the plugin directory
        content = os.listdir(pp)
        for fn in content:
            fp = os.path.join(pp, fn)
            if os.path.isfile(fp) and fn.endswith(".py"):
                # load module
                modname = os.path.splitext(fn)[0]
                mod = imp.load_source(modname, fp)
                # find implementations of interface
                for obj_name in dir(mod):
                    obj = getattr(mod, obj_name, None)
                    if interface.providedBy(obj):
                        # prevent duplicate insertions
                        pid = "{f}.{c}".format(f=fn, c=obj_name)
                        if pid not in plugin_identifiers:  # obj not in plugins wont work if the same plugin is installed multiple times
                            plugins.append(obj)
                            plugin_identifiers.append(pid)
                    
    return plugins


def make_global_dirs():
    """creates the global directoriesif they do not exist."""
    for p in PATHS:
        if not os.path.exists(p):
            os.makedirs(p)


# ================= INTERFACES ======================


class ICommand(Interface):
    """A shell command."""

    command = Attribute("""
    @type command: C{str} or C{unicode}
    @ivar command: the string to identify the command
    """)

    def on_load(factory, shell):
        """
        Called after the plugin was loaded.

        @type factory: C{ServerFactory}
        @param factory: the server factory

        @type shell: C{ManagementShell}
        @param shell: the command shell
        """
        pass

    def on_unload(factory, shell):
        """
        Called before the plugin will be unloaded.

        @type factory: C{ServerFactory}
        @param factory: the server factory

        @type shell: C{ManagementShell}
        @param shell: the command shell
        """
        pass

    def on_exit(shell):
        """
        Called when the shell is closed normaly.
        This may not be called when the shell is terminated.

        @type shell: C{ManagementShell}
        @param shell: the command shell
        """
        pass

    def on_cleanup(factory, shell):
        """
        Called when the cleanup command executes.

        @type factory: C{ServerFactory}
        @param factory: the server factory

        @type shell: C{ManagementShell}
        @param shell: the command shell
        """
        pass

    def run(shell, clients, line):
        """
        Runs the command.

        @type shell: C{ManagementShell}
        @param shell: the command shell

        @type clients: C{List} of C{ServerProtocol}
        @param clients: list of the currently selected clients

        @type line: C{str} or C{unicode}
        @param line: the arguments of the command (excluding ICommand.command)

        @rtype: C{bool}
        @return: whether to quit the shell or not.
        """
        pass


class IClientPlugin(Interface):
    """A client-side plugin."""

    plugin_id = Attribute("""
    @type plugin_id: C{str} or C{unicode}
    @param plugin_id: the string used to identify this plugin.
    """)

    def on_load(protocol):
        """
        Called after the plugin was loaded.

        @type protocol: C{ClientProtocol}
        @param protocol: The client-side protocol.
        """
        pass

    def on_unload(protocol):
        """
        Called before the plugin will be unloaded.

        @type protocol: C{ClientProtocol}
        @param protocol: The client-side protocol.
        """
        pass

    def on_disconnect(protocol):
        """
        Called when the connection was lost.

        @type protocol: C{ClientProtocol}
        @param protocol: The client-side protocol.
        """
        pass

    def on_connect(protocol):
        """
        Called when the connection was established.
        This may not be called on the first connect, as the plugin may not have been loaded at this time.

        @type protocol: C{ClientProtocol}
        @param protocol: The client-side protocol.
        """
        pass

    def on_cleanup(protocol):
        """
        Called when the cleanup command was received.

        @type protocol: C{ClientProtocol}
        @param protocol: The client-side protocol.
        """
        pass

    def on_remotecall(protocol, *args, **kwargs):
        """
        Called when a remotcall is being made to this plugin.

        @type protocol: C{ClientProtocol}
        @param protocol: The client-side protocol.
        """
        pass


# ================= AUTOCONNECT ======================


class AddressBroadcastProtocol(protocol.DatagramProtocol):
    """A protocol for broadcasting the port and ip of this host."""
    noisy = False

    def __init__(self):
        self._port = None
        self._ds = []
        self._loop = None

    def startProtocol(self):
        self.transport.setBroadcastAllowed(True)

    def start_broadcasting(self, port, interval=3):
        """starts broadcasting the ip/port"""
        if self._loop is not None:
            raise RuntimeError("Already broadcasting!")
        self._loop = task.LoopingCall(self.send_addrinfo, port)
        self._loop.start(interval=interval, now=True)

    def stop_broadcasting(self):
        """stops the broadcasting of the ip/port"""
        self._loop.stop()
        self._loop = None

    def send_addrinfo(self, port):
        """sends the address information."""
        self.transport.write(MAGIC + str(port), ("<broadcast>", BROADCAST_PORT))

    def datagramReceived(self, datagram, addr):
        if datagram.startswith(MAGIC):
            portinfo = datagram[len(MAGIC):]
            try:
                port = int(portinfo)
            except ValueError:
                return
            target = (addr[0], port)
            for d in self._ds:
                d.callback(target)
            self._ds = []

    def wait_for_addrinfo(self):
        """returns a deferred which will fire with a received (host, port)."""
        d = defer.Deferred()
        self._ds.append(d)
        return d


@defer.inlineCallbacks
def get_autoconnect_addr(reactor):
    """returns a deferred which will fire with a received (host, port)."""
    p = AddressBroadcastProtocol()
    port = reactor.listenUDP(BROADCAST_PORT, p)
    addr = yield p.wait_for_addrinfo()
    yield port.stopListening()
    defer.returnValue(addr)


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
                    "The connection was lost before a response was received."
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
    MAX_SINGLE_TRANSFER = 2 ** 15

    def make_temp_dirs(self):
        """create the tempdirs if they do not exists yet."""
        make_global_dirs()

    def is_path_allowed(self, p):
        """returns True if the path p is allowed to be accessed, False otherwise."""
        lap = os.path.abspath(DATA_DIR)
        ap = os.path.abspath(os.path.join(lap, p))
        return ap.startswith(lap)

    def _encode(self, data):
        """encodes data to be serializeable using json."""
        return base64.b64encode(data)

    def _decode(self, data):
        """decodes data encoded with _encode()"""
        return base64.b64decode(data)

    def disconnect(self):
        """disconnects the protocol."""
        self.transport.loseConnection()

    def remote_set_file_content(self, name, content):
        """sets the content of the file."""
        if os.path.isabs(name):
            p = name
        else:
            p = os.path.join(DATA_DIR, name)
        if not self.is_path_allowed(p):
            raise IOError("Path not allowed!")
        content = self._decode(content)
        with open(p, "wb") as fout:
            fout.write(content)

    def remote_add_to_file(self, name, content):
        """appends content to the specified file."""
        if os.path.isabs(name):
            p = name
        else:
            p = os.path.join(DATA_DIR, name)
        if not self.is_path_allowed(p):
            raise IOError("Path not allowed!")
        content = self._decode(content)
        with open(p, "ab") as fout:
            fout.write(content)

    @defer.inlineCallbacks
    def send_file(self, name, content):
        """sends a file to the peer."""
        # TODO: clean up the followinf if-statement
        if ((isinstance(content, str) and len(content) > self.MAX_SINGLE_TRANSFER) or (not isinstance(content, str))):
            if isinstance(content, str):
                iterator = chunked(content, self.MAX_SINGLE_TRANSFER)
            else:
                iterator = content
            n = 0
            for c in iterator:
                c = self._encode(c)
                n += 1
                if n == 1:
                    yield self.call_remote("set_file_content", name, c)
                else:
                    yield self.call_remote("add_to_file", name, c)
        else:
            yield self.call_remote("set_file_content", name, content)

    def send_version(self):
        """sends the version to the server."""
        self.call_action("version", {"version": VERSION})

    def remote_get_paths(self):
        """returns a dict containing the temp dir specifications of this client."""
        p = {
            "data": DATA_DIR,
            "sync": SYNC_DIR,
            "work": WORK_DIR,
            "plugins": PLUGIN_DIR,
            "cwd": os.getcwd(),
            }
        return p


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
        ServerAndClientSharedProtocol.connectionLost(self, reason)

    def run_command(self, command, per_core=False):
        """
        Runs the command on the client.
        If 'per_core' is nonzero, run the command once per core.
        """
        return self.call_remote("shell", command=command, per_core=per_core)

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

    def clear_data_dir(self):
        """clears the data directory of the client."""
        return self.call_remote("clear_data_dir")

    def cd(self, fn):
        """changes the CWD of the client."""
        return self.call_remote("cd", fn)

    def cd_work_dir(self):
        """changes the CWD of the client to the data dir."""
        return self.call_remote("cd", None)

    def get_cwd(self):
        """returns the CWD of the client."""
        return self.call_remote("getcwd")

    def get_paths(self):
        """returns a dict containing the temporary paths of the client."""
        return self.call_remote("get_paths")

    @defer.inlineCallbacks
    def send_dir(self, inp, outp):
        """sends the directory inp to outp on the client."""
        paths = yield self.get_paths()
        osp = paths["sync"]
        tzp = os.path.join(SYNC_DIR, random_filename())
        top = os.path.join(osp, random_filename())
        zip_dir(inp, tzp)
        with open(tzp, "rb") as f:
            iterator = filechunks(f, n=self.MAX_SINGLE_TRANSFER)
            yield self.send_file(top, iterator)
        yield self.unzip(top, outp)

    def unzip(self, inp, outp):
        """unzips inp into outp."""
        return self.call_remote("unzip", inp, outp)

    @defer.inlineCallbacks
    def deploy(self, inp, command, per_core=False):
        """
        Sends a directory to the client and runs a command.
        If 'per_core' is nonzero, 'command' is executed once per core.
        """
        paths = yield self.get_paths()
        workpath = paths["work"]
        cwd = paths["cwd"]
        yield self.send_dir(inp, workpath)
        yield self.cd_work_dir()
        ret = yield self.run_command(command, per_core=per_core)
        yield self.cd(cwd)
        defer.returnValue(ret)

    @defer.inlineCallbacks
    def send_plugins(self, inp):
        """sends the plugins installed in 'inp' to the client."""
        paths = yield self.get_paths()
        pp = paths["plugins"]
        yield self.send_dir(inp, pp)

    def stop(self):
        """stops the client."""
        self.factory.remove_client(self)
        return self.call_remote("stop")

    def load_plugins(self):
        """tells the client to load its plugins."""
        return self.call_remote("load_plugins")

    def unload_plugins(self):
        """tells the client to load its plugins."""
        return self.call_remote("unload_plugins")

    def call_plugin(self, pid, *args, **kwargs):
        """calls the plugin and returns a deferred which will fire with the return value."""
        return self.call_remote("call_plugin", pid, *args, **kwargs)


class ClientProtocol(ServerAndClientSharedProtocol):
    """The protocol for the client."""
    def __init__(self, factory, ns, d):
        ServerAndClientSharedProtocol.__init__(self)
        self.factory = factory
        self.ns = ns
        self.d = d
        self.is_working = False
        self.stopped = False
        self.tags = {}
        self.plugins = {}
        self.make_temp_dirs()

    def load_plugins(self):
        """loads the plugins."""
        self.plugins = {plugin.plugin_id: plugin for plugin in load_plugins(IClientPlugin)}
        self.call_plugins("on_load", self)

    remote_load_plugins = load_plugins  # alias for rpc

    def unload_plugins(self):
        """resets the loaded plugins and unregisters them."""
        self.call_plugins("on_unload", self)
        self.plugins = {}

    remote_unload_plugins = unload_plugins  # alias for rpc

    def call_plugins(self, fname, *args, **kwargs):
        """calls plugin.fname(*args, **kwargs) on all plugins"""
        for plugin_id in self.plugins:
            plugin = self.plugins[plugin_id]
            if hasattr(plugin, fname):
                f = getattr(plugin, fname)
                f(*args, **kwargs)


    def set_working(self, status=True):
        """sets the working status of the client."""
        self.is_working = status

    def connectionMade(self):
        """called when the connection was made."""
        ServerAndClientSharedProtocol.connectionMade(self)
        self.set_working(False)
        self.make_temp_dirs()
        self.call_plugins("on_connect", self)

    def connectionLost(self, reason):
        """called when the connection was lost."""
        ServerAndClientSharedProtocol.connectionLost(self, reason)
        self.call_plugins("on_disconnect", self)
        if (not self.ns.reconnect) and (not self.stopped):
            if isinstance(reason.value, ConnectionDone):
                self.d.callback(None)
            else:
                self.d.errback(reason)

    @defer.inlineCallbacks
    def remote_shell(self, command, per_core=False):
        """executes a shell command."""
        self.set_working(True)
        self.make_temp_dirs()
        if per_core:
            if multiprocessing is not None:
                n = multiprocessing.cpu_count()
            else:
                n = 1
        else:
            n = 1
        ds = []
        for i in xrange(n):
            d = utils.getProcessValue(command[0], command[1:])
            ds.append(d)
        ret = yield defer.gatherResults(ds)
        self.set_working(False)
        defer.returnValue(ret)

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
            "cwd": os.getcwd(),
            }
        if multiprocessing is not None:
            data["cores"] = multiprocessing.cpu_count()
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

    def remote_clear_data_dir(self):
        """clears the data dir."""
        delete_dir_content(DATA_DIR)
        self.call_plugins("on_cleanup", self)
        self.make_temp_dirs()

    def remote_cd(self, fn=None):
        """sets the working directory."""
        self.make_temp_dirs()
        if fn is None:
            fn = WORK_DIR
        os.chdir(fn)

    def remote_getcwd(self):
        """returns the currently working directory."""
        self.make_temp_dirs()
        return os.getcwd()

    def remote_unzip(self, inp, outp):
        """unzips inp into outp."""
        self.make_temp_dirs()
        with zipfile.ZipFile(inp, mode="r", allowZip64=True) as zf:
            zf.extractall(outp)

    def remote_stop(self):
        """stops this client"""
        self.factory.stop()
        self.disconnect()
        self.stopped = True
        self.d.callback(_STOP)

    def remote_call_plugin(self, pid, *args, **kwargs):
        """calls a plugin."""
        if pid not in self.plugins:
            raise PluginNotFoundError("Plugin '{n}' not found!".format(n=pid))
        plugin = self.plugins[pid]
        return plugin.on_remotecall(self, *args, **kwargs)


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
        self.clients[p.cid] = p

    def remove_client(self, p):
        """removes a client from the internal client list."""
        if p.cid in self.clients:
            del self.clients[p.cid]

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


class ClientFactory(protocol.Factory):
    """the factory for the client."""
    protocol = ClientProtocol

    def __init__(self, ns, d, service=None):
        self.ns = ns
        self.d = d
        self.service = service
        self.noisy = ns.verbose

    def buildProtocol(self, addr):
        """builds the protocol."""
        p = self.protocol(self, self.ns, self.d)
        return p

    def stop(self):
        """stops the factory"""
        if self.service is not None:
            self.service.stopService()


# ================= SHELL ======================

class ManagementShell(cmd.Cmd):
    """The management shell."""
    intro = "{n} v{v}\nType 'help' for help.".format(n=NAME, v=VERSION)

    def __init__(self, factory):
        cmd.Cmd.__init__(self)
        self.factory = factory
        self.plugins = []
        self.selected = []
        self.prompt = "(0 selected)"

    def load_plugins(self):
        """loads the plugins."""
        self.plugins = list(load_plugins(ICommand))
        self.call_plugins("on_load", self.factory, self)

    def unload_plugins(self):
        """resets the loaded plugins and unregisters them."""
        self.call_plugins("on_unload", self.factory, self)
        self.plugins = []

    def call_plugins(self, fname, *args, **kwargs):
        """calls plugin.fname(*args, **kwargs) on all plugins"""
        for plugin in self.plugins:
            if hasattr(plugin, fname):
                f = getattr(plugin, fname)
                f(*args, **kwargs)

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
        make_global_dirs()
        self.filter_selected()
        self.update_prompt()
        return line

    def postcmd(self, stop, line):
        """called when a command finished."""
        self.filter_selected()
        self.update_prompt()
        if stop:
            self.call_plugins("on_exit", self)
        return stop

    def default(self, line):
        """handles a line for which no do_* method exists."""
        splitted = shlex.split(line)
        command = splitted[0]
        arguments = splitted[1:]
        clients = [self.factory.get_client(cid) for cid in self.selected]
        f = False
        for plugin in self.plugins:
            if plugin.command == command:
                f = True
                plugin.run(self, clients, arguments)
                break
        if not f:
            return cmd.Cmd.default(self, line)

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

    @command_on_reactor_loop
    def do_stop(self, l):
        """stop: stops all selected clients."""
        for c in self.selected:
            p = self.factory.get_client(c)
            d = p.stop()
            d.addBoth(lambda r: None)

    @command_on_reactor_loop
    @defer.inlineCallbacks
    def do_remotecommand(self, l, per_core=False):
        """remotecommand [cmd]: executes cmd on the remote servers."""
        rc = shlex.split(l)
        ds = []
        clients = self.factory.get_clients(self.selected, include_None=False)
        for c in clients:
            d = c.run_command(rc, per_core=per_core)
            ds.append(d)
        if len(ds) > 0:
            codelists = yield defer.gatherResults(ds)
            codes = []
            for codelist in codelists:
                codes += codelist
            counter = collections.Counter(codes)
            self.write("Done. Exit codes:\n")
            self.pprint(dict(counter))
        else:
            self.write("Error: No clients found; no commands executed.\n")

    do_run = do_remotecommand
    def do_run_per_core(self, l):
        """run_per_core [cmd]: executes cmd on the remote servers n times in parallel, where n is the number of cpu cores of the client."""
        return self.do_run(l, per_core=True)

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

    @command_on_reactor_loop
    @defer.inlineCallbacks
    def do_cd(self, l):
        """cd [path]: changes the CWD of the selected clients. If 'path' is omitted, change into the DATA_DIR of the client."""
        if len(l) == 0:
            p = None
        else:
            p = l
        clients = self.factory.get_clients(self.selected, include_None=False)
        for c in clients:
            yield c.cd(p)

    @command_on_reactor_loop
    @defer.inlineCallbacks
    def do_cwd(self, l):
        """cwd: print the CWD."""
        ds = []
        clients = self.factory.get_clients(self.selected, include_None=False)
        for c in clients:
            d = c.get_cwd()
            ds.append(d)
        if len(ds) > 0:
            codes = yield defer.gatherResults(ds)
            counter = collections.Counter(codes)
            self.write("Done. CWDs:\n")
            self.pprint(dict(counter))
        else:
            self.write("Error: No clients found.\n")

    do_pwd = do_getcwd = do_cwd

    @command_on_reactor_loop
    @defer.inlineCallbacks
    def do_clear_data(self, l):
        """clear_data: removes all temporary files on the selected clients."""
        self.call_plugins("on_cleanup", self.factory, self)
        ds = []
        clients = self.factory.get_clients(self.selected, include_None=False)
        for c in clients:
            d = c.clear_data_dir()
            ds.append(d)
        if len(ds) > 0:
            yield defer.gatherResults(ds)
            self.write("Done\n")
        else:
            self.write("Error: No clients found.\n")

    do_cleanup = do_clear_data

    @command_on_reactor_loop
    @defer.inlineCallbacks
    def do_deploy(self, l, per_core=False):
        """deploy <dir> <command> [args [args...]]: send dir to selected clients and run command in the directory on the client."""
        rc = shlex.split(l)
        if len(rc) < 2:
            self.write("Error: expected at least two arguments!\n")
            defer.returnValue(None)
        p = rc[0]
        command = rc[1:]
        ds = []
        clients = self.factory.get_clients(self.selected, include_None=False)
        for c in clients:
            d = c.deploy(p, command, per_core=per_core)
            ds.append(d)
        if len(ds) > 0:
            codelists = yield defer.gatherResults(ds)
            codes = []
            for codelist in codelists:
                codes += codelist
            counter = collections.Counter(codes)
            self.write("Done. Exit codes:\n")
            self.pprint(dict(counter))
        else:
            self.write("Error: No clients found; no commands executed.\n")

    def do_deploy_per_core(self, l):
        """deploy <dir> <command> [args [args...]]: send dir to selected clients and run command n times parallel in the directory on the client, where n is the number of cpu_cores of the client."""
        return self.do_deploy(l, per_core=True)

    @command_on_reactor_loop
    @defer.inlineCallbacks
    def do_load_plugins(self, l):
        """load_plugins: loads the plugins."""
        self.load_plugins()
        ds = []
        for cid in self.selected:
            client = self.factory.get_client(cid)
            d = client.load_plugins()
            ds.append(d)
        yield defer.gatherResults(ds)

    @command_on_reactor_loop
    @defer.inlineCallbacks
    def do_unload_plugins(self, l):
        """unload_plugins: unloads the plugins."""
        self.unload_plugins()
        ds = []
        for cid in self.selected:
            client = self.factory.get_client(cid)
            d = client.unload_plugins()
            ds.append(d)
        yield defer.gatherResults(ds)

    def do_reload_plugins(self, l):
        """reload_plugins: reloads the plugins."""
        self.do_unload_plugins(l)
        self.do_send_plugins(l)
        self.do_load_plugins(l)

    def do_plugins(self, l):
        """plugins: shows the currently loaded plugins for the shell."""
        self.pprint(self.plugins)

    @command_on_reactor_loop
    @defer.inlineCallbacks
    def do_send_plugins(self, l):
        """sends the plugins to the selected clients."""
        clients = self.factory.get_clients(self.selected, include_None=False)
        ds = []
        for c in clients:
            d = c.send_plugins(HOME_PLUGIN_DIR)
            ds.append(d)
        if len(ds) > 0:
            yield defer.gatherResults(ds)
            self.write("Done.\n")
        else:
            self.write("Error: No clients found; no plugins sent.\n")

    def do_local_pyexec(self, l):
        """local_pyexec <cmd>: exec cmd on this shell."""
        exec(l)



# ================= MAIN CODE ======================

@defer.inlineCallbacks
def start_shell(reactor, ns):
    """launches a shell and returns a deferred, which will be fired when the shell is closed."""
    factory = ServerFactory()
    ep = endpoints.TCP4ServerEndpoint(reactor, interface="0.0.0.0", port=ns.port)
    ep.listen(factory)

    if ns.broadcast:
        broadcaster = AddressBroadcastProtocol()
        reactor.listenUDP(BROADCAST_PORT, broadcaster)
        broadcaster.start_broadcasting(port=ns.port, interval=ns.broadcast_interval)
    else:
        broadcaster = None
    
    if ns.shell:
        shell = ManagementShell(factory)
        d = threads.deferToThread(shell.cmdloop)
        yield d

    else:
        yield defer.Deferred()  # block forever

    if broadcaster is not None:
        broadcaster.stop_broadcasting()


@defer.inlineCallbacks
def start_client(reactor, ns):
    """starts the client and connects to the server."""
    connected = False
    while ((ns.autoconnect and ns.reconnect) or (not connected)):
        connected = True
        if ns.autoconnect:
            host, port = yield get_autoconnect_addr(reactor)
        else:
            host, port = ns.host, ns.port
        d = defer.Deferred()
        factory = ClientFactory(ns, d)
        ep = endpoints.TCP4ClientEndpoint(reactor, host=host, port=port)
        cs = ClientService(ep, factory, retryPolicy=lambda f: 3)
        factory.service = cs
        if not ns.reconnect:
            wfc = cs.whenConnected(failAfterFailures=1)
            wfc.addErrback(d.errback)
        cs.startService()
        state = yield d
        if state == _STOP:
            break


def main():
    """
    The main function.
    We start the server and a shell in a thread and wait for its completion.
    """
    make_global_dirs()

    parser = argparse.ArgumentParser(description="A python plugin-based cluster management system")
    parser.add_argument("action", action="store", choices=["server", "client"], help="what to do")
    parser.add_argument("-v", "--verbose", action="store_true", help="print additional information")
    parser.add_argument("-H", "--host", action="store", help="host/interface to connect/bind to", default="0.0.0.0")
    parser.add_argument("-p", "--port", action="store", type=int, default=DEFAULT_PORT, help="port of the server")
    parser.add_argument("-r", "--reconnect", action="store_true", dest="reconnect", help="auto reconnect to server")
    parser.add_argument("-b", "--broadcast", action="store_true", dest="broadcast", help="broadcast the ip/port of this server")
    parser.add_argument("-i", "--interval", action="store", dest="broadcast_interval", type=lambda x: max(int(x), 0.001), help="interval to broadcast address")
    parser.add_argument("-a", "--autoconnect", action="store_true", dest="autoconnect", help="autoconnect to broadcasted ip/ports")
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
