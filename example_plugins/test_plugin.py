"""simple plugin test."""

from zope.interface import implementer

from __main__ import ICommand

@implementer(ICommand)
class PrintingCommandPlugin(object):
    command = "plugin_test"
    
    def __init__(self, *args, **kwargs):
        self._log_call("__init__", args, kwargs)

    def _log_call(self, fname, args, kwargs):
        """logs a call."""
        msg = "{c}.{f}(*{a}, **{k})".format(c=self.__class__.__name__, f=fname, a=args, k=kwargs)
        print msg, id(self)

    def on_load(self, *args, **kwargs):
        self._log_call("on_load", args, kwargs)

    def on_unload(self, *args, **kwargs):
        self._log_call("on_unload", args, kwargs)

    def on_exit(self, *args, **kwargs):
        self._log_call("on_exit", args, kwargs)

    def on_cleanup(self, *args, **kwargs):
        self._log_call("on_cleanup", args, kwargs)

    def run(self, *args, **kwargs):
        self._log_call("run", args, kwargs)


p = PrintingCommandPlugin()
