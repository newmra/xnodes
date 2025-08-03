"""
xnodes: Exchange nodes framework
        Simplistic event framework which enables unrelated nodes to exchange information, alter each other states and
        provides the possibility to undo made changes.

Author: Ralph Neumann (@newmra)
"""

from xnodes import XEvent
from xnodes.x_core import publish_event_in_main_thread
from xnodes.i_x_main_thread_delegator import IXMainThreadDelegator


class XMainThreadDelegator(IXMainThreadDelegator):
    """
    Interface of classes which can delegate events to the main thread.
    """

    def _delegate_event_to_main_thread(self, event: XEvent) -> None:
        """
        Delegate an event to the main thread.
        :param event: Event to delegate.
        :return:
        """
        publish_event_in_main_thread(event)
