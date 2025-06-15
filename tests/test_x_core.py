"""
xnodes: Exchange nodes framework
        Simplistic event framework which enables unrelated nodes to exchange information, alter each other states and
        provides the possibility to undo made changes.

Author: Ralph Neumann (@newmra)
"""

import logging
import re
from typing import List
from unittest.mock import MagicMock, ANY, call

import pytest

import xnodes
from xnodes import x_core, XEventParameter, XCoreConfiguration, x_event_listener, XEvent, XMainThreadDelegator
from xnodes.x_core import EventPublishingContext, IMainThreadDelegator
from xnodes.x_event_description import XEventDescription
from xnodes.x_node_exception import XNodeException

NODE_ID_1 = "NODE_ID_1"
NODE_ID_2 = "NODE_ID_2"

EVENT_ID_1 = "EVENT_ID_1"
EVENT_ID_2 = "EVENT_ID_2"
EVENT_DESCRIPTION = "EVENT_DESCRIPTION"

EVENT_PARAMETER_NAME_1 = "parameter_1"
EVENT_PARAMETER_NAME_2 = "parameter_2"


# pylint: disable = protected-access


# noinspection PyProtectedMember
def _reset_x_core() -> None:
    """
    Reset the x_core variables and bring it into its initial state.
    :return: None
    """
    x_core._UNDO_STACK.clear()
    x_core._REDO_STACK.clear()

    x_core._NODE_IDS.clear()
    x_core._NODE_IDS.add(x_core.X_CORE_NODE_ID)

    x_core._EVENT_SUBSCRIPTIONS.clear()
    x_core._EVENT_SUBSCRIPTIONS[x_core.X_UNDO_EVENT].add(x_core.X_CORE_NODE_ID)
    x_core._EVENT_SUBSCRIPTIONS[x_core.X_REDO_EVENT].add(x_core.X_CORE_NODE_ID)
    x_core._EVENT_SUBSCRIPTIONS[x_core.X_CLEAR_UNDO_REDO_EVENTS].add(x_core.X_CORE_NODE_ID)

    x_core._EVENT_HANDLERS.clear()
    x_core._EVENT_HANDLERS.update({
        (x_core.X_UNDO_EVENT, x_core.X_CORE_NODE_ID):
            x_core.undo,
        (x_core.X_REDO_EVENT, x_core.X_CORE_NODE_ID):
            x_core.redo,
        (x_core.X_CLEAR_UNDO_REDO_EVENTS, x_core.X_CORE_NODE_ID):
            x_core._clear_undo_redo_stacks
    })

    x_core._EVENT_DESCRIPTIONS.clear()
    x_core._EVENT_DESCRIPTIONS.update({
        x_core.X_CORE_START:
            xnodes.x_event_description.XEventDescription(set(), x_core.logging.INFO),
        x_core.X_UNDO_EVENT:
            xnodes.x_event_description.XEventDescription(set(), x_core.logging.INFO),
        x_core.X_REDO_EVENT:
            xnodes.x_event_description.XEventDescription(set(), x_core.logging.INFO),

        # (Args: Undo counter, redo counter)
        x_core.X_MAP_UNDO_REDO_COUNTERS:
            xnodes.x_event_description.XEventDescription(
                {XEventParameter("undo_counter", int), XEventParameter("redo_counter", int)},
                x_core.logging.INFO),
        x_core.X_CLEAR_UNDO_REDO_EVENTS:
            xnodes.x_event_description.XEventDescription(set(), x_core.logging.INFO)
    })

    x_core._EVENT_LENGTH = 24
    x_core._IS_EVENT_IN_PROGRESS = False
    x_core._CONFIGURATION = XCoreConfiguration()


def test_register_event_raise_event_registered_twice() -> None:
    """
    Register an event and check that an exception is raised if the same event is registered twice.
    :return: None
    """
    _reset_x_core()

    x_core.register_event(EVENT_ID_1, {xnodes.XEventParameter(EVENT_PARAMETER_NAME_1)})

    with pytest.raises(XNodeException, match=re.escape(f"Attempted to register event '{EVENT_ID_1}' twice.")):
        x_core.register_event(EVENT_ID_1, {XEventParameter(EVENT_PARAMETER_NAME_1)})


def test_register_event_raise_invalid_log_level() -> None:
    """
    Register an event and check that an exception is raised if the log level has an invalid type.
    :return: None
    """
    _reset_x_core()

    with pytest.raises(XNodeException, match=re.escape(
            f"Attempted to register event '{EVENT_ID_1}', but the log_level is not of type 'int'.")):
        # noinspection PyTypeChecker
        x_core.register_event(EVENT_ID_1, [EVENT_PARAMETER_NAME_1], log_level="DEBUG")


def test_register_event_raise_parameters_not_iterable() -> None:
    """
    Register an event and check that an exception is raised if the parameters are not iterable.
    :return: None
    """
    _reset_x_core()

    with pytest.raises(TypeError, match=re.escape("'int' object is not iterable")):
        # noinspection PyTypeChecker
        x_core.register_event(EVENT_ID_1, 42)


def test_register_event_raise_invalid_parameter_type() -> None:
    """
    Register an event and check that invalid parameters are correctly recognized.
    :return: None
    """
    _reset_x_core()

    with pytest.raises(XNodeException, match=re.escape(
            f"Attempted to register event '{EVENT_ID_1}', but parameter 0 is not of type "
            f"'{XEventParameter.__name__}'.")):
        # noinspection PyTypeChecker
        x_core.register_event(EVENT_ID_1, {"Not a parameter"})


def test_register_event_raise_invalid_parameter_name_type() -> None:
    """
    Register an event and check that invalid parameters are correctly recognized.
    :return: None
    """
    _reset_x_core()

    with pytest.raises(XNodeException, match=re.escape(
            f"Attempted to register event '{EVENT_ID_1}', but parameter 0 has an invalid name, has to be "
            f"of type 'str'.")):
        # noinspection PyTypeChecker
        x_core.register_event(EVENT_ID_1, {XEventParameter(42)})


def test_register_event_raise_invalid_parameter_type_type() -> None:
    """
    Register an event and check that invalid parameters are correctly recognized.
    :return: None
    """
    _reset_x_core()

    with pytest.raises(XNodeException, match=re.escape(
            f"Attempted to register event '{EVENT_ID_1}', but parameter 0 has an invalid type, has to be "
            f"of type 'type' or None.")):
        # noinspection PyTypeChecker
        x_core.register_event(EVENT_ID_1, {XEventParameter(EVENT_PARAMETER_NAME_1, "str")})


def test_register_event_raise_invalid_parameter_description_type() -> None:
    """
    Register an event and check that invalid parameters are correctly recognized.
    :return: None
    """
    _reset_x_core()

    with pytest.raises(XNodeException, match=re.escape(
            f"Attempted to register event '{EVENT_ID_1}', but parameter 0 has an invalid description, has "
            f"to be of type 'str'.")):
        # noinspection PyTypeChecker
        x_core.register_event(EVENT_ID_1, {XEventParameter(EVENT_PARAMETER_NAME_1, str, 42)})


def test_register_event_raise_sender_id_in_parameters() -> None:
    """
    Register an event and check that an exception is raised if the sender ID is used as a parameter name.
    :return: None
    """
    _reset_x_core()

    with pytest.raises(XNodeException, match=re.escape(
            f"Attempted to register event '{EVENT_ID_1}', but parameter {x_core._SENDER_ID_PARAMETER_NAME} "
            f"is a reserved name.")):
        x_core.register_event(EVENT_ID_1, {XEventParameter(x_core._SENDER_ID_PARAMETER_NAME, str, "")})


@pytest.mark.parametrize("parameter", [
    XEventParameter(EVENT_ID_1), XEventParameter(EVENT_ID_1, description=EVENT_DESCRIPTION),
    XEventParameter(EVENT_ID_1, int),
    XEventParameter(EVENT_ID_1, int, EVENT_DESCRIPTION)
], ids=[
    "Only parameter name", "Parameter name with description", "Parameter name with type",
    "Parameter name with type and description"
])
def test_register_event_valid_parameter(parameter) -> None:
    """
    Register an event and check that valid parameters can be set without a raised exception.
    :param parameter: Parameter to test.
    :return: None
    """
    _reset_x_core()

    x_core.register_event(EVENT_ID_1, {parameter})


def test_register_event_raise_duplicated_parameter() -> None:
    """
    Register an event and check that an exception is raised if a parameter is added twice.
    :return: None
    """
    _reset_x_core()

    with pytest.raises(XNodeException, match=re.escape(
            f"Attempted to register event '{EVENT_ID_1}', but parameter {EVENT_PARAMETER_NAME_1} is "
            "configured twice.")):
        x_core.register_event(EVENT_ID_1,
                              {XEventParameter(EVENT_PARAMETER_NAME_1), XEventParameter(EVENT_PARAMETER_NAME_1, str)})


def test_register_node_raise_node_registered_twice() -> None:
    """
    Test 'register_node' and check that an exception is raised if a node ID is registered twice.
    :return: None
    """
    _reset_x_core()

    class Node:
        """
        Dummy node.
        """

        def empty_method(self) -> None:
            """
            Empty method.
            :return: None
            """

    node = Node()
    x_core.register_node(NODE_ID_1, node)

    with pytest.raises(XNodeException, match=re.escape(
            f"Attempted to register node '{NODE_ID_1}', but a node with that ID is already registered.")):
        x_core.register_node(NODE_ID_1, node)


def test_register_node_raise_invalid_event() -> None:
    """
    Test 'register_node' and check that an exception is raised if a registered node handles an event which was not
    registered before.
    :return: None
    """
    _reset_x_core()

    class Node:
        """
        Dummy node.
        """

        @x_event_listener(EVENT_ID_1)
        def handler_invalid(self) -> None:
            """
            Dummy handler.
            :return: None
            """

    with pytest.raises(XNodeException, match=re.escape(
            f"Node '{NODE_ID_1}' handles event '{EVENT_ID_1}', but the event is not registered.")):
        x_core.register_node(NODE_ID_1, Node())


def test_register_node_raise_invalid_event_parameters() -> None:
    """
    Test 'register_node' and check that an exception is raised if a node handles an event, but the event parameter do
    not match with the parameters with which the event were registered with.
    :return: None
    """
    _reset_x_core()

    x_core.register_event(EVENT_ID_1, {XEventParameter(EVENT_PARAMETER_NAME_1)})

    class Node:
        """
        Dummy node.
        """

        @x_event_listener(EVENT_ID_1)
        def handler_invalid(self, parameter_2) -> None:
            """
            Dummy handler.
            :param parameter_2: Test parameter.
            :return: None
            """

    with pytest.raises(XNodeException, match=re.escape(
            f"Node '{NODE_ID_1}' handles event '{EVENT_ID_1}', but the parameters do not match. "
            f"Event requires: ['{EVENT_PARAMETER_NAME_1}'], handler provides: ['parameter_2'].")):
        x_core.register_node(NODE_ID_1, Node())


def test_register_node_no_exception() -> None:
    """
    Test 'register_node' and check that a node is registered without issues.
    :return: None
    """
    _reset_x_core()

    x_core.register_event(EVENT_ID_1,
                          {XEventParameter(EVENT_PARAMETER_NAME_1), XEventParameter(EVENT_PARAMETER_NAME_2)})

    class Node:
        """
        Dummy node.
        """

        @x_event_listener(EVENT_ID_1)
        def handler_invalid(self, parameter_1, parameter_2) -> None:
            """
            Dummy handler.
            :param parameter_1: Parameter 1.
            :param parameter_2: Parameter 2.
            :return: None
            """

    x_core.register_node(NODE_ID_1, Node())


def test_register_node_raise_append_sender_id_but_no_argument() -> None:
    """
    Test 'register_node' and check that n exception is raised if the sender ID shall be appended, but the required
    argument is missing.
    :return: None
    """
    _reset_x_core()

    x_core.register_event(EVENT_ID_1,
                          {XEventParameter(EVENT_PARAMETER_NAME_1), XEventParameter(EVENT_PARAMETER_NAME_2)})

    class Node:
        """
        Dummy node.
        """

        @x_event_listener(EVENT_ID_1, append_sender_id=True)
        def handler_invalid(self) -> None:
            """
            Dummy handler.
            :return: None
            """

    with pytest.raises(XNodeException, match=re.escape(
            f"Node '{NODE_ID_1}' handles event '{EVENT_ID_1}', but the event listener does not "
            "request the sender id in its decorator.")):
        x_core.register_node(NODE_ID_1, Node())


def test_unregister_node_raise_invalid_node() -> None:
    """
    Test 'unregister_node' and check that an exception is raised if a node is unregistered which was not registered
    before.
    :return: None
    """
    _reset_x_core()

    with pytest.raises(
            XNodeException,
            match=re.escape(
                f"Attempted to unregister node '{NODE_ID_1}', but no node with that ID is registered.")
    ):
        x_core.unregister_node(NODE_ID_1)


def test_unregister_node() -> None:
    """
    Test 'unregister_node' and check that a node can be unregistered successfully.
    :return: None
    """
    _reset_x_core()

    x_core.register_event(EVENT_ID_1, set())
    x_core.register_event(EVENT_ID_2, set())

    class Node:
        """
        Dummy node.
        """

        @x_event_listener(EVENT_ID_2)
        def handler(self) -> None:
            """
            Dummy handler.
            :return: None
            """

    x_core.register_node(NODE_ID_1, Node())
    x_core.unregister_node(NODE_ID_1)


def test_start_raise_invalid_maximum_logging_length() -> None:
    """
    Test 'start' and check that an exception is raised if a configuration with a too small
    'id_maximum_logging_length' is provided.
    :return: None
    """
    _reset_x_core()

    configuration = XCoreConfiguration(id_maximum_logging_length=0)

    with pytest.raises(
            XNodeException,
            match=re.escape(
                "Invalid configuration: 'id_maximum_logging_length' has to be greater or equal to 10.")):
        x_core.start(configuration)


def test_start_raise_invalid_main_thread_delegator() -> None:
    """
    Test 'start' and check that an exception is raised if the main thread delegator has an invalid type.
    :return: None
    """
    _reset_x_core()

    with pytest.raises(
            XNodeException,
            match=re.escape("Main thread delegator has to be of type 'IMainThreadDelegator'.")):
        # noinspection PyTypeChecker
        x_core.start(main_thread_delegator=42)


def test_start(monkeypatch) -> None:
    """
    Test 'start' and check that the x_core is started successfully.
    :param monkeypatch: Monkeypatch.
    :return: None
    """
    _reset_x_core()

    broadcast_mock = MagicMock()
    monkeypatch.setattr(x_core, "broadcast", broadcast_mock)

    x_core.start()
    broadcast_mock.assert_called_once_with(x_core.X_CORE_START, x_core.X_CORE_NODE_ID, {})


def test_publish_raise_event_not_registered() -> None:
    """
    Test 'publish' and check that an exception is raised if the event is not registered.
    :return: None
    """
    _reset_x_core()

    with pytest.raises(XNodeException,
                       match=re.escape(
                           f"Node '{NODE_ID_1}' attempted to publish event '{EVENT_ID_1}' to node "
                           f"'{NODE_ID_2}', but the event is not registered.")):
        x_core.publish(EVENT_ID_1, NODE_ID_1, NODE_ID_2, {})


def test_publish_raise_sender_not_registered() -> None:
    """
    Test 'publish' and check that an exception is raised if the sender node is not registered.
    :return: None
    """
    _reset_x_core()

    x_core.register_event(EVENT_ID_1, set())

    with pytest.raises(XNodeException,
                       match=re.escape(
                           f"Node '{NODE_ID_1}' attempted to publish event '{EVENT_ID_1}' to node "
                           f"'{NODE_ID_2}', but the sender node is not registered.")):
        x_core.publish(EVENT_ID_1, NODE_ID_1, NODE_ID_2, {})


def test_publish_raise_receiver_not_registered() -> None:
    """
    Test 'publish' and check that an exception is raised if the receiver is not registered.
    :return: None
    """
    _reset_x_core()

    class Node:
        """
        Dummy node.
        """

    x_core.register_event(EVENT_ID_1, set())
    x_core.register_node(NODE_ID_1, Node())

    with pytest.raises(XNodeException,
                       match=re.escape(
                           f"Node '{NODE_ID_1}' attempted to publish event '{EVENT_ID_1}' to node "
                           f"'{NODE_ID_2}', but the receiver node is not registered.")):
        x_core.publish(EVENT_ID_1, NODE_ID_1, NODE_ID_2, {})


def test_publish_raise_receiver_not_handles_event() -> None:
    """
    Test 'publish' and check that an exception is raised if the receiver does not handle the event.
    :return: None
    """
    _reset_x_core()

    class Node:
        """
        Dummy node.
        """

    x_core.register_event(EVENT_ID_1, set())
    x_core.register_node(NODE_ID_1, Node())
    x_core.register_node(NODE_ID_2, Node())

    with pytest.raises(XNodeException,
                       match=re.escape(
                           f"Node '{NODE_ID_1}' attempted to publish event '{EVENT_ID_1}' to node "
                           f"'{NODE_ID_2}', but receiver '{NODE_ID_2}' is not subscribed to event "
                           f"'{EVENT_ID_1}'.")):
        x_core.publish(EVENT_ID_1, NODE_ID_1, NODE_ID_2, {})


def test_publish(monkeypatch) -> None:
    """
    Test 'publish' and check that an event is successfully published to the receiver.
    :return: None
    """
    _reset_x_core()

    class Node:
        """
        Dummy node.
        """

        @x_event_listener(EVENT_ID_1)
        def handler(self) -> None:
            """
            Dummy handler.
            :return: None
            """

    x_core.register_event(EVENT_ID_1, set())
    x_core.register_node(NODE_ID_1, Node())
    x_core.register_node(NODE_ID_2, Node())

    publish_events_mock = MagicMock()
    monkeypatch.setattr(x_core, "publish_events", publish_events_mock)

    built_event = "BUILT_EVENT"
    build_event_mock = MagicMock()
    build_event_mock.return_value = built_event
    monkeypatch.setattr(x_core, "_build_event", build_event_mock)

    x_core.publish(EVENT_ID_1, NODE_ID_1, NODE_ID_2, {})
    build_event_mock.assert_called_once_with(EVENT_ID_1, NODE_ID_1, NODE_ID_2, {})
    publish_events_mock.assert_called_once_with([built_event], is_undo=False)


def test_broadcast_raise_event_not_registered() -> None:
    """
    Test 'broadcast' and check that an exception is raised if the event is not registered.
    :return: None
    """
    _reset_x_core()

    with pytest.raises(
            XNodeException,
            match=re.escape(
                f"Node '{NODE_ID_1}' attempted to broadcast event '{EVENT_ID_1}', but the event is not "
                f"registered.")):
        x_core.broadcast(EVENT_ID_1, NODE_ID_1, {})


def test_broadcast_raise_sender_not_registered() -> None:
    """
    Test 'broadcast' and check that an exception is raised if the sender is not registered.
    :return: None
    """
    _reset_x_core()

    x_core.register_event(EVENT_ID_1, set())

    with pytest.raises(
            XNodeException,
            match=re.escape(
                f"Node '{NODE_ID_1}' attempted to broadcast event '{EVENT_ID_1}', but the sender node "
                "is not registered.")):
        x_core.broadcast(EVENT_ID_1, NODE_ID_1, {})


def test_broadcast(monkeypatch) -> None:
    """
    Test 'broadcast' and check that the event is broadcast.
    :return: None
    """
    _reset_x_core()

    class Node:
        """
        Dummy node.
        """

        @x_event_listener(EVENT_ID_1)
        def handler(self) -> None:
            """
            Dummy handler.
            :return: None
            """

    x_core.register_event(EVENT_ID_1, set())
    x_core.register_node(NODE_ID_1, MagicMock())
    x_core.register_node(NODE_ID_2, Node())

    publish_events_mock = MagicMock()
    monkeypatch.setattr(x_core, "publish_events", publish_events_mock)

    log_mock = MagicMock()
    monkeypatch.setattr(x_core, "_log", log_mock)

    built_event = "BUILT_EVENT"
    build_event_mock = MagicMock()
    build_event_mock.return_value = built_event
    monkeypatch.setattr(x_core, "_build_event", build_event_mock)

    x_core.broadcast(EVENT_ID_1, NODE_ID_1, {})
    build_event_mock.assert_has_calls([call(EVENT_ID_1, NODE_ID_1, NODE_ID_2, {}),
                                       call(EVENT_ID_1, NODE_ID_1, "BROADCAST", {})])
    log_mock.assert_called_once()
    publish_events_mock.assert_called_once_with([built_event], is_undo=False)


def test_broadcast_no_receiver(monkeypatch) -> None:
    """
    Test 'broadcast' and check that nothing happens if no node is subscribed to the event.
    :param monkeypatch: Monkeypatch.
    :return: None
    """
    _reset_x_core()

    x_core.register_event(EVENT_ID_1, set())
    x_core.register_node(NODE_ID_1, MagicMock())

    publish_events_mock = MagicMock()
    monkeypatch.setattr(x_core, "publish_events", publish_events_mock)

    build_event_mock = MagicMock()
    monkeypatch.setattr(x_core, "_build_event", build_event_mock)

    logger_warning_mock = MagicMock()
    monkeypatch.setattr(x_core.LOGGER, "warning", logger_warning_mock)

    log_mock = MagicMock()
    monkeypatch.setattr(x_core, "_log", log_mock)

    x_core.broadcast(EVENT_ID_1, NODE_ID_1, {})
    build_event_mock.assert_called_once_with(EVENT_ID_1, NODE_ID_1, "BROADCAST", {})
    log_mock.assert_called_once()
    publish_events_mock.assert_not_called()


def test_add_undo_events(monkeypatch) -> None:
    """
    Test 'add_undo_events' and check the redo stack is cleared if an undo event is added.
    :param monkeypatch: Monkeypatch.
    :return: None
    """
    _reset_x_core()

    x_core._REDO_STACK.append([XEvent("TEST", XEventDescription(set()), "", "", {})])

    append_undo_events_mock = MagicMock()
    monkeypatch.setattr(x_core, "_append_undo_events", append_undo_events_mock)

    publish_undo_redo_counters_mock = MagicMock()
    monkeypatch.setattr(x_core, "_publish_undo_redo_counters", publish_undo_redo_counters_mock)

    x_core.add_undo_events([])
    assert len(x_core._REDO_STACK) == 0
    append_undo_events_mock.assert_called_once_with([])
    publish_undo_redo_counters_mock.assert_called_once()


def test_undo_events(monkeypatch) -> None:
    """
    Test '_undo_events' and check the undo events are published.
    :param monkeypatch: Monkeypatch.
    :return: None
    """
    _reset_x_core()

    x_core._UNDO_STACK.append([XEvent("TEST", XEventDescription(set()), "", "", {})])

    publish_events_mock = MagicMock()
    monkeypatch.setattr(x_core, "publish_events", publish_events_mock)

    x_core.undo()
    publish_events_mock.assert_called_once_with(ANY, is_undo=True)


def test_undo_events_no_undo_events(monkeypatch) -> None:
    """
    Test '_undo_events' and check that nothing happens if not undo events are available.
    :param monkeypatch: Monkeypatch.
    :return: None
    """
    _reset_x_core()

    publish_events_mock = MagicMock()
    monkeypatch.setattr(x_core, "publish_events", publish_events_mock)

    x_core.undo()
    publish_events_mock.assert_not_called()


def test_redo_events(monkeypatch) -> None:
    """
    Test '_redo_events' and check the redo events are published.
    :param monkeypatch: Monkeypatch.
    :return: None
    """
    _reset_x_core()

    x_core._REDO_STACK.append([XEvent("TEST", XEventDescription(set()), "", "", {})])

    publish_events_mock = MagicMock()
    monkeypatch.setattr(x_core, "publish_events", publish_events_mock)

    x_core.redo()
    publish_events_mock.assert_called_once_with(ANY, is_undo=False)


def test_redo_events_no_redo_events(monkeypatch) -> None:
    """
    Test '_redo_events' and check that nothing happens if not redo events are available.
    :param monkeypatch: Monkeypatch.
    :return: None
    """
    _reset_x_core()

    publish_events_mock = MagicMock()
    monkeypatch.setattr(x_core, "publish_events", publish_events_mock)

    x_core.redo()
    publish_events_mock.assert_not_called()


def test_append_undo_events() -> None:
    """
    Test '_append_undo_events' and check that undo events are appended.
    :return: None
    """
    _reset_x_core()

    event = XEvent("TEST", XEventDescription(set()), "", "", {})

    x_core._REDO_STACK.append([event])
    x_core._REDO_STACK.append([event])
    x_core._REDO_STACK.append([event])

    x_core._append_undo_event([event])
    assert len(x_core._REDO_STACK) == 3
    assert len(x_core._UNDO_STACK) == 1


def test_append_undo_remove_oldest_undo_event() -> None:
    """
    Test '_append_undo_events' and check that the oldest undo event is deleted if there are more undo events than
    configured.
    :return: None
    """
    _reset_x_core()

    first_event = XEvent("TEST", XEventDescription(set()), "", "", {})
    other_event = XEvent("TEST", XEventDescription(set()), "", "", {})

    x_core._UNDO_STACK.append([first_event])
    for _ in range(998):
        x_core._UNDO_STACK.append([other_event])

    assert len(x_core._UNDO_STACK) == 999
    x_core._append_undo_event([first_event])
    assert len(x_core._UNDO_STACK) == 1000
    x_core._append_undo_event([other_event])
    assert len(x_core._UNDO_STACK) == 1000
    assert x_core._UNDO_STACK[0][0] is not first_event


def test_append_undo_keep_all_undo_events() -> None:
    """
    Test '_append_undo_events' and check that no undo event is deleted if 'maximum_undo_events' is less than 0.
    :return: None
    """
    _reset_x_core()

    x_core._CONFIGURATION = XCoreConfiguration(maximum_undo_events=-1)

    event = XEvent("TEST", XEventDescription(set()), "", "", {})

    events_to_add = 100000
    for i in range(events_to_add):
        assert len(x_core._UNDO_STACK) == i
        x_core._append_undo_event([event])
        assert len(x_core._UNDO_STACK) == i + 1


def test_clear_undo_redo_stack(monkeypatch) -> None:
    """
    Test '_clear_undo_redo_stacks' and check that the stacks are cleared.
    :param monkeypatch: Monkeypatch.
    :return: None
    """
    _reset_x_core()

    publish_undo_redo_counters_mock = MagicMock()
    monkeypatch.setattr(x_core, "_publish_undo_redo_counters", publish_undo_redo_counters_mock)

    event = XEvent("TEST", XEventDescription(set()), "", "", {})

    x_core._UNDO_STACK.append([event])
    x_core._REDO_STACK.append([event])

    x_core._clear_undo_redo_stacks()
    assert len(x_core._UNDO_STACK) == 0
    assert len(x_core._REDO_STACK) == 0
    publish_undo_redo_counters_mock.assert_called_once()


def test_publish_undo_redo_counters(monkeypatch) -> None:
    """
    Test '_publish_undo_redo_counters' and check that the undo and redo counters are published.
    :param monkeypatch: Monkeypatch.
    :return: None
    """
    _reset_x_core()

    broadcast_mock = MagicMock()
    monkeypatch.setattr(x_core, "broadcast", broadcast_mock)

    event = XEvent(EVENT_ID_1, XEventDescription(set()), "", "", {})

    undo_count = 21
    redo_count = 21

    for _ in range(undo_count):
        x_core._UNDO_STACK.append([event])

    for _ in range(redo_count):
        x_core._REDO_STACK.append([event])

    x_core._publish_undo_redo_counters()
    broadcast_mock.assert_called_once_with(x_core.X_MAP_UNDO_REDO_COUNTERS, x_core.X_CORE_NODE_ID, {
        "undo_counter": undo_count,
        "redo_counter": redo_count
    })


def test_build_event_raise_event_not_registered() -> None:
    """
    Test '_build_event' and check that an exception is raised if the event is not registered.
    :return: None
    """
    _reset_x_core()

    with pytest.raises(XNodeException,
                       match=re.escape(f"Attempted to create an unknown event '{EVENT_ID_1}'.")):
        x_core._build_event(EVENT_ID_1, NODE_ID_1, NODE_ID_2, {})


def test_build_event_raise_event_parameter_not_matching() -> None:
    """
    Test '_build_event' and check that an exception is raised if the event parameters do not match.
    :return: None
    """
    _reset_x_core()

    registered_parameter_1 = "parameter_1"
    registered_parameter_2 = "parameter_2"
    provided_parameter_1 = "parameter_3"
    provided_parameter_2 = "parameter_4"

    x_core.register_event(EVENT_ID_1,
                          {XEventParameter(registered_parameter_1), XEventParameter(registered_parameter_2, bool)})

    with pytest.raises(XNodeException,
                       match="Event 'event' cannot be constructed, event requires: \\['parameter_1|2', "
                             "'parameter_1|2'\\], provided are: \\['parameter_3|4', 'parameter_3|4'\\]\\."):
        x_core._build_event(EVENT_ID_1, NODE_ID_1, NODE_ID_2, {
            provided_parameter_1: 42,
            provided_parameter_2: "test"
        })


def test_build_event() -> None:
    """
    Test '_build_event' and check that an event is successfully built.
    :return: None
    """
    _reset_x_core()

    x_core.register_event(EVENT_ID_1,
                          {XEventParameter(EVENT_PARAMETER_NAME_1), XEventParameter(EVENT_PARAMETER_NAME_2, bool)})

    event = x_core._build_event(EVENT_ID_1, NODE_ID_1, NODE_ID_2, {
        EVENT_PARAMETER_NAME_1: 42,
        EVENT_PARAMETER_NAME_2: "test"
    })

    assert event.id == EVENT_ID_1
    assert event.sender_id == NODE_ID_1
    assert event.receiver_id == NODE_ID_2


def test_log(monkeypatch) -> None:
    """
    Test '_log' and check that the event is logged.
    :param monkeypatch: Monkeypatch.
    :return: None
    """
    _reset_x_core()

    base_logging_string = "BASE"
    parameters_logging_string = "PARAMETERS"

    create_base_logging_string_mock = MagicMock()
    create_base_logging_string_mock.return_value = base_logging_string
    monkeypatch.setattr(x_core, "_create_base_logging_string", create_base_logging_string_mock)

    create_parameters_logging_string_mock = MagicMock()
    create_parameters_logging_string_mock.return_value = parameters_logging_string + "        "
    monkeypatch.setattr(x_core, "_create_parameters_logging_string", create_parameters_logging_string_mock)

    log_mock = MagicMock()
    monkeypatch.setattr(x_core.LOGGER, "log", log_mock)

    event_mock = MagicMock()
    event_mock.event_description.log_level = logging.INFO

    x_core._log(event_mock)
    log_mock.assert_called_once_with(logging.INFO, f"{base_logging_string}{parameters_logging_string}")


def test_create_base_logging_string(monkeypatch) -> None:
    """
    Test '_create_base_logging_string' and check that the base logging string is created correctly.
    :param monkeypatch: Monkeypatch.
    :return: None
    """
    _reset_x_core()

    id_maximum_logging_length = 20

    configuration_mock = MagicMock()
    configuration_mock.id_maximum_logging_length = id_maximum_logging_length
    configuration_mock.log_event_parameters = False
    monkeypatch.setattr(x_core, "_CONFIGURATION", configuration_mock)

    event_mock = MagicMock()
    event_mock.sender_id = NODE_ID_1
    event_mock.receiver_id = NODE_ID_2
    event_mock.id = EVENT_ID_1
    event_mock.event_description.log_level = logging.INFO

    base_logging_string = x_core._create_base_logging_string(event_mock)
    assert base_logging_string == f"           {NODE_ID_1} --------- {EVENT_ID_1} --------> {NODE_ID_2}           "


def test_create_parameters_logging_string(monkeypatch) -> None:
    """
    Test '_create_parameters_logging_string' and check that the parameters logging string is created correctly.
    :param monkeypatch: Monkeypatch.
    :return: None
    """
    _reset_x_core()

    configuration_mock = MagicMock()
    configuration_mock.id_maximum_logging_length = 10
    configuration_mock.event_parameter_maximum_logging_length = 30
    configuration_mock.log_event_parameters = True
    monkeypatch.setattr(x_core, "_CONFIGURATION", configuration_mock)

    event_mock = MagicMock()
    event_mock.event_description.log_level = logging.INFO
    event_mock.event_description.parameters = {XEventParameter(EVENT_PARAMETER_NAME_1, int)}
    event_mock.parameters = {
        EVENT_PARAMETER_NAME_1: "21"
    }

    assert x_core._create_parameters_logging_string(event_mock) == f" | {EVENT_PARAMETER_NAME_1} (int / str): '21'"


def test_log_with_parameters_empty_if_disabled(monkeypatch) -> None:
    """
    Test '_create_parameters_logging_string' and check that an empty string is returned if logging of event parameters
    is disabled.
    :param monkeypatch: Monkeypatch.
    :return: None
    """
    _reset_x_core()

    configuration_mock = MagicMock()
    configuration_mock.log_event_parameters = False
    monkeypatch.setattr(x_core, "_CONFIGURATION", configuration_mock)

    event_mock = MagicMock()
    assert x_core._create_parameters_logging_string(event_mock) == ""


def test_log_with_parameters_empty_if_no_parameters_are_available(monkeypatch) -> None:
    """
    Test '_create_parameters_logging_string' and check that an empty string is returned if no parameters are available.
    :param monkeypatch: Monkeypatch.
    :return: None
    """
    _reset_x_core()

    configuration_mock = MagicMock()
    configuration_mock.log_event_parameters = True
    monkeypatch.setattr(x_core, "_CONFIGURATION", configuration_mock)

    event_mock = MagicMock()
    event_mock.event_description.parameters = set()
    assert x_core._create_parameters_logging_string(event_mock) == ""


def test_publish_events_no_undo_events_but_is_undo(monkeypatch) -> None:
    """
    Test '_publish_events' and check that no redo event is added if no redo event is provided.
    :param monkeypatch: Monkeypatch.
    :return: None
    """
    _reset_x_core()

    event_mock = MagicMock()
    event_mock.receiver_id = NODE_ID_1

    monkeypatch.setattr(x_core, "EventPublishingContext", MagicMock())
    monkeypatch.setattr(x_core, "_log", MagicMock())
    monkeypatch.setattr(x_core, "_execute_event", MagicMock())
    monkeypatch.setattr(x_core, "_extract_undo_events", MagicMock())

    publish_undo_redo_counters_mock = MagicMock()
    monkeypatch.setattr(x_core, "_publish_undo_redo_counters", publish_undo_redo_counters_mock)

    assert len(x_core._REDO_STACK) == 0
    x_core.publish_events([event_mock], is_undo=True)
    assert len(x_core._REDO_STACK) == 0
    publish_undo_redo_counters_mock.assert_called_once()


def test_publish_events_no_undo_events_is_not_undo(monkeypatch) -> None:
    """
    Test '_publish_events' and check that no undo and redo counters are published if no undo events are provided.
    :param monkeypatch: Monkeypatch.
    :return: None
    """
    _reset_x_core()

    event_mock = MagicMock()
    event_mock.receiver_id = NODE_ID_1

    monkeypatch.setattr(x_core, "_log", MagicMock())
    monkeypatch.setattr(x_core, "_execute_event", MagicMock())
    monkeypatch.setattr(x_core, "_extract_undo_events", MagicMock())

    event_publishing_context_mock = MagicMock()
    monkeypatch.setattr(x_core, "EventPublishingContext", event_publishing_context_mock)

    publish_undo_redo_counters_mock = MagicMock()
    monkeypatch.setattr(x_core, "_publish_undo_redo_counters", publish_undo_redo_counters_mock)

    x_core.publish_events([event_mock], is_undo=False)
    publish_undo_redo_counters_mock.assert_not_called()


def test_publish_events_undo_events_is_undo(monkeypatch) -> None:
    """
    Test '_publish_events' and check that the undo and redo counters are published if a redo event is added.
    :param monkeypatch: Monkeypatch.
    :return: None
    """
    _reset_x_core()

    event_mock = MagicMock()
    event_mock.receiver_id = NODE_ID_1

    monkeypatch.setattr(x_core, "EventPublishingContext", MagicMock())
    monkeypatch.setattr(x_core, "_log", MagicMock())
    monkeypatch.setattr(x_core, "_execute_event", MagicMock())

    extract_undo_events_mock = MagicMock()
    extract_undo_events_mock.return_value = [event_mock]
    monkeypatch.setattr(x_core, "_extract_undo_events", extract_undo_events_mock)

    publish_undo_redo_counters_mock = MagicMock()
    monkeypatch.setattr(x_core, "_publish_undo_redo_counters", publish_undo_redo_counters_mock)

    assert len(x_core._REDO_STACK) == 0
    x_core.publish_events([event_mock], is_undo=True)
    assert len(x_core._REDO_STACK) == 1
    publish_undo_redo_counters_mock.assert_called_once()


def test_publish_events_undo_events_is_not_undo(monkeypatch) -> None:
    """
    Test '_publish_events' and check that an undo event is added if an event is published.
    :param monkeypatch: Monkeypatch.
    :return: None
    """
    _reset_x_core()

    event_mock = MagicMock()
    event_mock.receiver_id = NODE_ID_1

    monkeypatch.setattr(x_core, "EventPublishingContext", MagicMock())
    monkeypatch.setattr(x_core, "_log", MagicMock())
    monkeypatch.setattr(x_core, "_execute_event", MagicMock())

    extract_undo_events_mock = MagicMock()
    extract_undo_events_mock.return_value = [event_mock]
    monkeypatch.setattr(x_core, "_extract_undo_events", extract_undo_events_mock)

    publish_undo_redo_counters_mock = MagicMock()
    monkeypatch.setattr(x_core, "_publish_undo_redo_counters", publish_undo_redo_counters_mock)

    append_undo_events_mock = MagicMock()
    monkeypatch.setattr(x_core, "_append_undo_events", append_undo_events_mock)

    assert len(x_core._REDO_STACK) == 0
    x_core.publish_events([event_mock], is_undo=False)
    assert len(x_core._REDO_STACK) == 0
    append_undo_events_mock.assert_called_once()
    publish_undo_redo_counters_mock.assert_called_once()


def test_publish_events_delegate_to_main_thread(monkeypatch) -> None:
    """
    Test '_publish_events' and check that the event is delegated to the main thread.
    :param monkeypatch: Monkeypatch.
    :return: None
    """
    _reset_x_core()

    class MainThreadDelegator(XMainThreadDelegator):

        def __init__(self):
            self.events = []
            self.is_undo = False

        def delegate_events(self, events: List[XEvent], is_undo: bool) -> None:
            self.events = events
            self.is_undo = is_undo

    test_events = ["EVENT_1", "EVENT_2"]

    main_thread_delegator = MainThreadDelegator()
    monkeypatch.setattr(x_core, "_MAIN_THREAD_DELEGATOR", main_thread_delegator)

    is_main_thread_mock = MagicMock()
    is_main_thread_mock.return_value = False
    monkeypatch.setattr(x_core, "_is_main_thread", is_main_thread_mock)

    # noinspection PyTypeChecker
    x_core.publish_events(test_events, False)

    assert main_thread_delegator.events == test_events
    assert not main_thread_delegator.is_undo


def test_publish_events_raise_not_main_thread_and_no_delegator(monkeypatch) -> None:
    """
    Test '_publish_events' and check that an exception is raised if events are published not in the main thread and no
    delegator is set.
    :param monkeypatch: Monkeypatch.
    :return: None
    """
    _reset_x_core()

    is_main_thread_mock = MagicMock()
    is_main_thread_mock.return_value = False
    monkeypatch.setattr(x_core, "_is_main_thread", is_main_thread_mock)

    with pytest.raises(XNodeException, match=re.escape(
            "Attempted to broadcast events outside of the main thread, with no main thread delegator set.")):
        x_core.publish_events([], False)


def test_publish_events_in_main_thread_raise_not_main_thread(monkeypatch) -> None:
    """
    Test 'publish_events_in_main_thread' and check that an exception is raised if events are published not in the main
    thread.
    :param monkeypatch: Monkeypatch.
    :return: None
    """
    _reset_x_core()

    is_main_thread_mock = MagicMock()
    is_main_thread_mock.return_value = False
    monkeypatch.setattr(x_core, "_is_main_thread", is_main_thread_mock)

    with pytest.raises(XNodeException, match=re.escape(
            "Attempted to publish events outside of the main thread.")):
        x_core.publish_events_in_main_thread([], False)


def test_i_main_thread_delegator_interface() -> None:
    """
    Test 'IMainThreadDelegator' and check that the interface raises an exception if the method is not implemented.
    :return: None
    """
    main_thread_delegator = IMainThreadDelegator()
    with pytest.raises(NotImplementedError):
        main_thread_delegator.delegate_events([], False)
    with pytest.raises(NotImplementedError):
        main_thread_delegator._delegate_events_to_main_thread([], False)


def test_execute_event_raise_event_not_subscribed() -> None:
    """
    Tst '_execute_event' and check that an exception is raised if an event is executed which was not registered.
    :return: None
    """
    _reset_x_core()

    event_mock = MagicMock()
    event_mock.id = EVENT_ID_1
    event_mock.receiver_id = NODE_ID_1

    with pytest.raises(
            XNodeException,
            match=re.escape(
                f"Attempted to send event with ID '{EVENT_ID_1}' to node '{NODE_ID_1}', but "
                "the node is not subscribed to that event.")):
        x_core._execute_event(event_mock)


def test_execute_event() -> None:
    """
    Test '_execute_event' and check that the correct event handler of a node is called if an event is executed.
    :return: None
    """
    _reset_x_core()

    parameter_1_value = 21
    parameter_2_value = 42

    parameter_description = {XEventParameter(EVENT_PARAMETER_NAME_2, int), XEventParameter(EVENT_PARAMETER_NAME_1, int)}

    class Node:
        """
        Test node.
        """

        def __init__(self):
            """
            Initialize the node.
            """
            self.is_called = False

        @x_event_listener(EVENT_ID_1, append_sender_id=True)
        def handler(self, parameter_1: int, parameter_2: int, sender_id: str):
            """
            Test handler.
            :param parameter_1: Parameter 1.
            :param parameter_2: Parameter 2.
            :param sender_id: Sender ID.
            :return: None
            """
            self.is_called = True

            assert parameter_1 == parameter_1_value
            assert parameter_2 == parameter_2_value
            assert sender_id == NODE_ID_2

    node = Node()
    x_core.register_event(EVENT_ID_1, parameter_description)
    x_core.register_node(NODE_ID_1, node)

    event_mock = MagicMock()
    event_mock.id = EVENT_ID_1
    event_mock.sender_id = NODE_ID_2
    event_mock.receiver_id = NODE_ID_1
    event_mock.event_description = XEventDescription(parameter_description)
    event_mock.parameters = {
        EVENT_PARAMETER_NAME_1: parameter_1_value,
        EVENT_PARAMETER_NAME_2: parameter_2_value
    }

    x_core._execute_event(event_mock)
    assert node.is_called


def test_extract_undo_events_not_a_generator() -> None:
    """
    Test '_extract_undo_events' and check that no undo events are created if the undo event is not a generator.
    :return: None
    """
    _reset_x_core()

    assert len(x_core._extract_undo_event(None, "")) == 0


@pytest.mark.parametrize("undo_event", [None, (None,), (None, None, None)],
                         ids=["Not a tuple", "Too few elements", "Too many elements"])
def test_extract_undo_events_invalid_undo_event_type(undo_event) -> None:
    """
    Test '_extract_undo_events' and check that an exception is raised if the undo event has an invalid type.
    :param undo_event: Undo event to test.
    :return: None
    """
    _reset_x_core()

    with pytest.raises(XNodeException,
                       match=re.escape("Undo event has to be a tuple consisting of the event and the parameters.")):
        x_core._extract_undo_event(iter([undo_event]), "")


def test_extract_undo_events_raise_invalid_parameters_type() -> None:
    """
    Test '_extract_undo_events' and check that an exception is raised if the undo parameter have an invalid type.
    :return: None
    """
    _reset_x_core()

    with pytest.raises(XNodeException,
                       match=re.escape("Undo event parameters has an invalid type, should be dict, is: 'int'.")):
        x_core._extract_undo_event(iter([("test", 42)]), "")


def test_extract_undo_events(monkeypatch) -> None:
    """
    Test '_extract_undo_events' and check that an undo event is built.
    :param monkeypatch: Monkeypatch.
    :return: None
    """
    _reset_x_core()

    parameters = {
        "test": 42
    }

    build_event_mock = MagicMock()
    build_event_mock.return_value = MagicMock()
    monkeypatch.setattr(x_core, "_build_event", build_event_mock)

    assert len(x_core._extract_undo_event(iter([(EVENT_ID_1, parameters)]), NODE_ID_1)) == 1
    build_event_mock.assert_called_once_with(EVENT_ID_1, NODE_ID_1, NODE_ID_1, parameters)


def test_event_publishing_context_no_log_if_logging_level_too_high(monkeypatch) -> None:
    """
    Test 'EventPublishingContext' and check that no empty line is logged if no log level of any event is higher than
    the configured log level.
    :param monkeypatch: Monkeypatch.
    :return: None
    """
    _reset_x_core()

    event_mock = MagicMock()
    event_mock.event_description.log_level = logging.DEBUG

    log_mock = MagicMock()
    monkeypatch.setattr(x_core.LOGGER, "log", log_mock)

    configuration_mock = MagicMock()
    configuration_mock.log_level = logging.INFO
    monkeypatch.setattr(x_core, "_CONFIGURATION", configuration_mock)

    with EventPublishingContext([event_mock]):
        pass

    log_mock.assert_not_called()


def test_event_publishing_context(monkeypatch) -> None:
    """
    Test 'EventPublishingContext' and check if an empty line is logged only once if the context is nested.
    :param monkeypatch: Monkeypatch.
    :return: None
    """
    _reset_x_core()

    event_mock = MagicMock()
    event_mock.event_description.log_level = logging.DEBUG

    log_mock = MagicMock()
    monkeypatch.setattr(x_core.LOGGER, "log", log_mock)

    configuration_mock = MagicMock()
    configuration_mock.log_level = logging.DEBUG
    monkeypatch.setattr(x_core, "_CONFIGURATION", configuration_mock)

    with EventPublishingContext([event_mock]):
        # Second context within the first context should not log an empty line again.
        with EventPublishingContext([event_mock]):
            pass

    log_mock.assert_called_once()
