"""simple plugin test."""

from zope.interface import implementer

from twisted.internet.defer import inlineCallbacks, gatherResults
from twisted.internet.utils import getProcessOutput

from __main__ import ICommand, IClientPlugin


PLUGIN_ID = "disk_usage_plugin"

@implementer(ICommand)
class DiskUsageCommand(object):
    command = "disk_usage"
    
    def __init__(self, *args, **kwargs):
        pass

    def on_load(self, *args, **kwargs):
        pass

    def on_unload(self, *args, **kwargs):
        pass

    def on_exit(self, *args, **kwargs):
        pass

    def on_cleanup(self, *args, **kwargs):
        pass

    @inlineCallbacks
    def run(self, shell, clients, arguments):
        ds = []
        for client in clients:
            d = client.call_plugin(PLUGIN_ID)
            ds.append(d)
        results = yield gatherResults(ds)
        sep = "=" * 50
        for client, result in zip(clients, results):
            shell.write("{s}\nClient {c}:\n{r}\n".format(s=sep, c=client.cid, r=result))
        shell.write("\nDone.\n")


@implementer(IClientPlugin)
class DiskUsagePlugin(object):
    plugin_id = PLUGIN_ID

    def on_load(self, protocol):
        pass

    def on_unload(self, protocol):
        pass

    def on_disconnect(self, protocol):
        pass

    def on_connect(self, protocol):
        pass

    def on_cleanup(self, protocol):
        pass

    def on_remotecall(self, protocol, *args, **kwargs):
        return getProcessOutput("df", args=["-h"])


command = DiskUsageCommand()
plugin = DiskUsagePlugin()
