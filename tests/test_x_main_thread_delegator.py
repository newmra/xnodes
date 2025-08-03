"""
xnodes: Exchange nodes framework
        Simplistic event framework which enables unrelated nodes to exchange information, alter each other states and
        provides the possibility to undo made changes.

Author: Ralph Neumann (@newmra)
"""

from unittest.mock import MagicMock

from xnodes import x_main_thread_delegator


def test_main_thread_delegation(monkeypatch):
    """
    Test the delegation of events to the main thread.
    :param monkeypatch: Monkeypatch.
    :return: None
    """
    event_1 = "event_1"

    publish_event_in_main_thread_mock = MagicMock()
    monkeypatch.setattr(x_main_thread_delegator, "publish_event_in_main_thread", publish_event_in_main_thread_mock)

    main_thread_delegator = x_main_thread_delegator.XMainThreadDelegator()

    # noinspection PyTypeChecker
    main_thread_delegator._delegate_event_to_main_thread(event_1)
    publish_event_in_main_thread_mock.assert_called_once_with(event_1)
