"""
xnodes: Exchange nodes framework
        Simplistic event framework which enables unrelated nodes to exchange information, alter each other states and
        provides the possibility to undo made changes.

Author: Ralph Neumann (@newmra)
"""
import contextlib
import logging
import re
from unittest.mock import MagicMock, ANY

import pytest

import xnodes
from xnodes import x_core, XEventParameter, XCoreConfiguration, x_event_listener, X_MAP_UNDO_REDO_COUNTERS
from xnodes.i_x_main_thread_delegator import IXMainThreadDelegator
from xnodes.x_core import X_CORE_NODE_ID, EventPublishingContext
from xnodes.x_event import EventType, XEvent
from xnodes.x_event_description import XEventDescription
from xnodes.x_node_exception import XNodeException

NODE_ID_1 = "NODE_ID_1"
NODE_ID_2 = "NODE_ID_2"

EVENT_ID_1 = "EVENT_ID_1"
EVENT_ID_2 = "EVENT_ID_2"
EVENT_DESCRIPTION = "EVENT_DESCRIPTION"

EVENT_PARAMETER_NAME_1 = "parameter_1"
EVENT_PARAMETER_NAME_2 = "parameter_2"

TEST_EVENT = XEvent("TEST", XEventDescription(set()), "", "", {})


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
    x_core._EVENT_HANDLERS.clear()

    x_core._EVENT_DESCRIPTIONS.clear()
    x_core._EVENT_DESCRIPTIONS.update({
        x_core.X_CORE_START:
            xnodes.x_event_description.XEventDescription(set(), x_core.logging.INFO),

        # (Args: Undo counter, redo counter)
        x_core.X_MAP_UNDO_REDO_COUNTERS:
            xnodes.x_event_description.XEventDescription(
                {XEventParameter("undo_counter", int), XEventParameter("redo_counter", int)},
                x_core.logging.INFO),
    })

    x_core._MINIMUM_ID_MAXIMUM_LOGGING_LENGTH = 10
    x_core._LAST_EVENT_LOG_LENGTH = 0
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

    class Node1:
        """
        Dummy node.
        """

        @x_event_listener(EVENT_ID_1)
        def handler(self) -> None:
            """
            Dummy handler.
            :return: None
            """

    class Node2:
        """
        Dummy node.
        """

        @x_event_listener(EVENT_ID_2)
        def handler(self) -> None:
            """
            Dummy handler.
            :return: None
            """

    x_core.register_node(NODE_ID_1, Node1())
    x_core.register_node(NODE_ID_2, Node2())
    x_core.unregister_node(NODE_ID_2)


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
            match=re.escape("Main thread delegator has to be of type 'IXMainThreadDelegator'.")):
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

    publish_event_mock = MagicMock()
    monkeypatch.setattr(x_core, "_publish_event", publish_event_mock)

    built_event = "BUILT_EVENT"
    build_event_mock = MagicMock()
    build_event_mock.return_value = built_event
    monkeypatch.setattr(x_core, "_build_event", build_event_mock)

    x_core.publish(EVENT_ID_1, NODE_ID_1, NODE_ID_2, {})
    build_event_mock.assert_called_once_with(EVENT_ID_1, NODE_ID_1, NODE_ID_2, {}, EventType.DO, )
    publish_event_mock.assert_called_once_with(built_event)


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

    publish_event_mock = MagicMock()
    monkeypatch.setattr(x_core, "_publish_event", publish_event_mock)

    log_mock = MagicMock()
    monkeypatch.setattr(x_core, "_log", log_mock)

    built_event = "BUILT_EVENT"
    build_event_mock = MagicMock()
    build_event_mock.return_value = built_event
    monkeypatch.setattr(x_core, "_build_event", build_event_mock)

    x_core.broadcast(EVENT_ID_1, NODE_ID_1, {})
    build_event_mock.assert_called_once_with(EVENT_ID_1, NODE_ID_1, NODE_ID_2, {}, EventType.DO, is_broadcast=True)
    log_mock.assert_not_called()
    publish_event_mock.assert_called_once_with(built_event)


def test_broadcast_no_receiver(monkeypatch) -> None:
    """
    Test 'broadcast' and check that nothing happens if no node is subscribed to the event.
    :param monkeypatch: Monkeypatch.
    :return: None
    """
    _reset_x_core()

    x_core.register_event(EVENT_ID_1, set())
    x_core.register_node(NODE_ID_1, MagicMock())

    publish_event_mock = MagicMock()
    monkeypatch.setattr(x_core, "_publish_event", publish_event_mock)

    build_event_mock = MagicMock()
    monkeypatch.setattr(x_core, "_build_event", build_event_mock)

    logger_warning_mock = MagicMock()
    monkeypatch.setattr(x_core.LOGGER, "warning", logger_warning_mock)

    log_mock = MagicMock()
    monkeypatch.setattr(x_core, "_log", log_mock)

    x_core.broadcast(EVENT_ID_1, NODE_ID_1, {})
    build_event_mock.assert_not_called()
    log_mock.assert_not_called()
    publish_event_mock.assert_not_called()


def test__add_undo_event__raise_invalid_event_id() -> None:
    _reset_x_core()

    with pytest.raises(XNodeException, match=re.escape(
            f"Attempted to add an undo event '{EVENT_ID_1}' addressed to node "
            f"'{NODE_ID_1}', but the event is not registered."
    )):
        x_core.add_undo_event(EVENT_ID_1, NODE_ID_1)


def test__add_undo_event__raise_invalid_receiver_id() -> None:
    _reset_x_core()

    x_core._EVENT_DESCRIPTIONS[EVENT_ID_1] = XEventDescription(set())

    with pytest.raises(XNodeException, match=re.escape(
            f"Attempted to add an undo event '{EVENT_ID_1}' addressed to node "
            f"'{NODE_ID_1}', but the receiver node is not registered."
    )):
        x_core.add_undo_event(EVENT_ID_1, NODE_ID_1)


def test__add_undo_event__raise_receiver_not_subscribed() -> None:
    _reset_x_core()

    x_core._EVENT_DESCRIPTIONS[EVENT_ID_1] = XEventDescription(set())
    x_core._NODE_IDS.add(NODE_ID_1)

    with pytest.raises(XNodeException, match=re.escape(
            f"Attempted to add an undo event '{EVENT_ID_1}' addressed to node "
            f"'{NODE_ID_1}', but receiver is not subscribed to event."
    )):
        x_core.add_undo_event(EVENT_ID_1, NODE_ID_1)


def test__add_undo_event__undo_event_added(monkeypatch) -> None:
    _reset_x_core()

    x_core._EVENT_DESCRIPTIONS[EVENT_ID_1] = XEventDescription(set())
    x_core._NODE_IDS.add(NODE_ID_1)
    x_core._EVENT_HANDLERS[(EVENT_ID_1, NODE_ID_1)] = MagicMock()
    x_core._REDO_STACK.append(MagicMock())

    build_event_mock = MagicMock()
    monkeypatch.setattr(x_core, "_build_event", build_event_mock)

    broadcast_mock = MagicMock()
    monkeypatch.setattr(x_core, "broadcast", broadcast_mock)

    build_event_mock.return_value = MagicMock()

    # Adding a new undo event should remove any existing redo events, because the
    assert len(x_core._REDO_STACK) != 0
    assert len(x_core._UNDO_STACK) == 0

    x_core.add_undo_event(EVENT_ID_1, NODE_ID_1)
    build_event_mock.assert_called_once_with(EVENT_ID_1, x_core.X_CORE_NODE_ID, NODE_ID_1, {}, EventType.UNDO)
    broadcast_mock.assert_called_once_with(X_MAP_UNDO_REDO_COUNTERS, X_CORE_NODE_ID, ANY)

    assert len(x_core._REDO_STACK) == 0
    assert len(x_core._UNDO_STACK) != 0


def test__undo__do_nothing_if_no_undo_events(monkeypatch) -> None:
    """
    Test 'undo' and check that nothing happens if no undo events are available.
    :param monkeypatch: Monkeypatch.
    :return: None
    """
    _reset_x_core()

    publish_event_mock = MagicMock()
    monkeypatch.setattr(x_core, "_publish_event", publish_event_mock)

    x_core.undo()
    publish_event_mock.assert_not_called()


def test__undo__publish_undo_event(monkeypatch) -> None:
    """
    Test 'undo' and check that the undo event is published.
    :param monkeypatch: Monkeypatch.
    :return: None
    """
    _reset_x_core()

    x_core._UNDO_STACK.append(MagicMock())

    publish_event_mock = MagicMock()
    monkeypatch.setattr(x_core, "_publish_event", publish_event_mock)

    x_core.undo()
    publish_event_mock.assert_called_once()


def test__redo__do_nothing_if_no_redo_events(monkeypatch) -> None:
    """
    Test 'redo' and check that nothing happens if no redo events are available.
    :param monkeypatch: Monkeypatch.
    :return: None
    """
    _reset_x_core()

    publish_event_mock = MagicMock()
    monkeypatch.setattr(x_core, "_publish_event", publish_event_mock)

    x_core.redo()
    publish_event_mock.assert_not_called()


def test__redo__publish_redo_event(monkeypatch) -> None:
    """
    Test 'redo' and check that the redo event is published.
    :param monkeypatch: Monkeypatch.
    :return: None
    """
    _reset_x_core()

    x_core._REDO_STACK.append(MagicMock())

    publish_event_mock = MagicMock()
    monkeypatch.setattr(x_core, "_publish_event", publish_event_mock)

    x_core.redo()
    publish_event_mock.assert_called_once()


def test_append_undo_remove_oldest_undo_event() -> None:
    """
    Test '_append_undo_events' and check that the oldest undo event is deleted if there are more undo events than
    configured.
    :return: None
    """
    _reset_x_core()

    first_event = XEvent("TEST", XEventDescription(set()), "", "", {})
    other_event = XEvent("TEST", XEventDescription(set()), "", "", {})

    x_core._UNDO_STACK.append(first_event)
    for _ in range(998):
        x_core._UNDO_STACK.append(other_event)

    assert len(x_core._UNDO_STACK) == 999
    x_core._append_undo_event(other_event)
    assert len(x_core._UNDO_STACK) == 1000
    x_core._append_undo_event(other_event)
    assert len(x_core._UNDO_STACK) == 1000
    assert x_core._UNDO_STACK[0] is not first_event


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
        x_core._append_undo_event(event)
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

    x_core._UNDO_STACK.append(event)
    x_core._REDO_STACK.append(event)

    x_core.clear_undo_redo_stacks()
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
        x_core._UNDO_STACK.append(event)

    for _ in range(redo_count):
        x_core._REDO_STACK.append(event)

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
        x_core._build_event(EVENT_ID_1, NODE_ID_1, NODE_ID_2, {}, EventType.DO)


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
        },
                            EventType.DO)


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
    }, EventType.DO)

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
        x_core._publish_event(MagicMock())


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
        x_core.publish_event_in_main_thread(MagicMock())


def test_i_main_thread_delegator_interface() -> None:
    """
    Test 'IMainThreadDelegator' and check that the interface raises an exception if the method is not implemented.
    :return: None
    """
    main_thread_delegator = IXMainThreadDelegator()
    with pytest.raises(NotImplementedError):
        main_thread_delegator.delegate_event(MagicMock())


def test_execute_event_raise_event_not_subscribed() -> None:
    """
    Tst '_execute_event' and check that an exception is raised if an event is executed which was not registered.
    :return: None
    """
    _reset_x_core()

    event_mock = MagicMock()
    event_mock.event_type = EventType.DO
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
    event_mock.event_type = EventType.DO
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


def test_execute_event__log_error_if_original_event_type_is_unknown(monkeypatch):
    logger_error_mock = MagicMock()
    monkeypatch.setattr(x_core.LOGGER, "error", logger_error_mock)

    event_mock = MagicMock()
    event_mock.event_type.value = "UNKNOWN"

    assert x_core._execute_event(event_mock) is None

    logger_error_mock.assert_called_once_with("**** XNODES INTERNAL FRAMEWORK ERROR **** Unknown event type: UNKNOWN.")


def test_execute_event__raise_undo_event_no_tuple():
    _reset_x_core()

    def test_generator():
        yield 42

    event_mock = MagicMock()
    event_mock.event_type = EventType.DO
    event_mock.sender_id = NODE_ID_1
    event_mock.receiver_id = NODE_ID_2
    event_mock.id = EVENT_ID_1

    x_core._EVENT_HANDLERS[(EVENT_ID_1, NODE_ID_2)] = test_generator

    with pytest.raises(XNodeException, match=re.escape(
            "Attempted to set an undo event, but the undo event data is not a tuple.")):
        x_core._execute_event(event_mock)


def test_execute_event__raise_undo_event_not_a_tuple_of_two_elements():
    _reset_x_core()

    def test_generator():
        yield 42, 21, 1

    event_mock = MagicMock()
    event_mock.event_type = EventType.DO
    event_mock.sender_id = NODE_ID_1
    event_mock.receiver_id = NODE_ID_2
    event_mock.id = EVENT_ID_1

    x_core._EVENT_HANDLERS[(EVENT_ID_1, NODE_ID_2)] = test_generator

    with pytest.raises(XNodeException, match=re.escape(
            "Attempted to set an undo event, but the undo event data has to many values to unpack, expected "
            "are 2 (Event-ID, Parameters-Dict) values, provided are 3 values.")):
        x_core._execute_event(event_mock)


def test_execute_event__raise_event_id_not_a_string():
    _reset_x_core()

    def test_generator():
        yield 42, 21

    event_mock = MagicMock()
    event_mock.event_type = EventType.DO
    event_mock.sender_id = NODE_ID_1
    event_mock.receiver_id = NODE_ID_2
    event_mock.id = EVENT_ID_1

    x_core._EVENT_HANDLERS[(EVENT_ID_1, NODE_ID_2)] = test_generator

    with pytest.raises(XNodeException, match=re.escape(
            "Attempted to set an undo event, but undo event is not a str, is: 'int'.")):
        x_core._execute_event(event_mock)


def test_execute_event__raise_event_is_not_registered():
    _reset_x_core()

    def test_generator():
        yield EVENT_ID_1, 21

    event_mock = MagicMock()
    event_mock.event_type = EventType.DO
    event_mock.sender_id = NODE_ID_1
    event_mock.receiver_id = NODE_ID_2
    event_mock.id = EVENT_ID_1

    x_core._EVENT_HANDLERS[(EVENT_ID_1, NODE_ID_2)] = test_generator

    with pytest.raises(XNodeException, match=re.escape(
            f"Attempted to set an undo event, but undo event '{EVENT_ID_1}' is not registered.")):
        x_core._execute_event(event_mock)


def test_execute_event__raise_parameters_not_a_dict():
    _reset_x_core()

    def test_generator():
        yield EVENT_ID_2, 21

    event_mock = MagicMock()
    event_mock.event_type = EventType.DO
    event_mock.sender_id = NODE_ID_1
    event_mock.receiver_id = NODE_ID_2
    event_mock.id = EVENT_ID_1

    x_core._EVENT_HANDLERS[(EVENT_ID_1, NODE_ID_2)] = test_generator
    x_core._EVENT_DESCRIPTIONS[EVENT_ID_2] = XEventDescription(set())

    with pytest.raises(XNodeException, match=re.escape(
            "Attempted to set an undo event, but undo event parameters is not a dict, is: 'int'.")):
        x_core._execute_event(event_mock)


def test_execute_event__raise_multiple_undo_events_yielded():
    _reset_x_core()

    def test_generator():
        yield EVENT_ID_1, dict()
        yield EVENT_ID_2, dict()

    event_mock = MagicMock()
    event_mock.event_type = EventType.DO
    event_mock.sender_id = NODE_ID_1
    event_mock.receiver_id = NODE_ID_2
    event_mock.id = EVENT_ID_1

    x_core._EVENT_HANDLERS[(EVENT_ID_1, NODE_ID_2)] = test_generator
    x_core._EVENT_DESCRIPTIONS[EVENT_ID_1] = XEventDescription(set())
    x_core._EVENT_DESCRIPTIONS[EVENT_ID_2] = XEventDescription(set())

    with pytest.raises(XNodeException, match=re.escape(
            "Attempted to set an undo event, but an event can only return a single undo event, a second "
            "one was yielded.")):
        x_core._execute_event(event_mock)


@pytest.mark.parametrize("original_event_type, undo_event_type", [
    (EventType.DO, EventType.UNDO),
    (EventType.REDO, EventType.UNDO),
    (EventType.UNDO, EventType.REDO)
])
def test_execute_event__build_undo_event_from_do_event(
        original_event_type: EventType, undo_event_type: EventType, monkeypatch):
    _reset_x_core()

    event_instance_mock = MagicMock()

    build_event_mock = MagicMock()
    build_event_mock.return_value = event_instance_mock
    monkeypatch.setattr(x_core, "_build_event", build_event_mock)

    undo_event_parameters = {
        "a": 42,
        "b": 21
    }

    def test_generator():
        yield EVENT_ID_1, undo_event_parameters

    event_mock = MagicMock()
    event_mock.event_type = original_event_type
    event_mock.sender_id = NODE_ID_1
    event_mock.receiver_id = NODE_ID_2
    event_mock.id = EVENT_ID_1

    x_core._EVENT_HANDLERS[(EVENT_ID_1, NODE_ID_2)] = test_generator
    x_core._EVENT_DESCRIPTIONS[EVENT_ID_1] = XEventDescription(set())
    x_core._execute_event(event_mock)

    build_event_mock.assert_called_once_with(EVENT_ID_1, NODE_ID_2, NODE_ID_2, undo_event_parameters, undo_event_type)


def test_event_publishing_context_single_event(monkeypatch) -> None:
    """
    Test EventPublishingContext with a single event, verifying state changes and logging on exit.
    """
    _reset_x_core()

    log_info_mock = MagicMock()
    monkeypatch.setattr(x_core.LOGGER, "info", log_info_mock)

    x_core._CONFIGURATION.log_level = logging.DEBUG

    assert not x_core._IS_EVENT_IN_PROGRESS

    with EventPublishingContext() as context:
        assert context.is_first_event_in_batch
        assert x_core._IS_EVENT_IN_PROGRESS
        assert x_core._LAST_EVENT_LOG_LENGTH == -1

        x_core._LAST_EVENT_LOG_LENGTH = 50

    log_info_mock.assert_called_once_with("*" * 50)
    assert not x_core._IS_EVENT_IN_PROGRESS


def test_event_publishing_context_nested_events(monkeypatch) -> None:
    """
    Test EventPublishingContext with nested events, verifying only the outermost context logs and resets state.
    """
    _reset_x_core()

    log_info_mock = MagicMock()
    monkeypatch.setattr(x_core.LOGGER, "info", log_info_mock)

    x_core._CONFIGURATION.log_level = logging.DEBUG

    assert not x_core._IS_EVENT_IN_PROGRESS

    with EventPublishingContext() as outer_context:
        assert outer_context.is_first_event_in_batch
        assert x_core._IS_EVENT_IN_PROGRESS
        assert x_core._LAST_EVENT_LOG_LENGTH == -1

        with EventPublishingContext() as inner_context:
            assert not inner_context.is_first_event_in_batch
            assert x_core._IS_EVENT_IN_PROGRESS

        log_info_mock.assert_not_called()
        assert x_core._IS_EVENT_IN_PROGRESS

        x_core._LAST_EVENT_LOG_LENGTH = 70

    log_info_mock.assert_called_once_with("*" * 70)
    assert not x_core._IS_EVENT_IN_PROGRESS


def test_event_publishing_context_exit_no_log_if_log_level_too_low(monkeypatch) -> None:
    """
    Test EventPublishingContext does not log on exit if the event's log level is below the configuration's threshold.
    """
    _reset_x_core()

    log_mock = MagicMock()
    monkeypatch.setattr(x_core.LOGGER, "log", log_mock)

    x_core._CONFIGURATION.log_level = logging.WARNING

    with EventPublishingContext():
        assert x_core._IS_EVENT_IN_PROGRESS
        x_core._LAST_EVENT_LOG_LENGTH = 50

    log_mock.assert_not_called()
    assert not x_core._IS_EVENT_IN_PROGRESS


def test_event_publishing_context_exit_no_log_if_last_log_length_negative(monkeypatch) -> None:
    """
    Test EventPublishingContext does not log on exit if no logging occurred within the context
    (i.e., _LAST_EVENT_LOG_LENGTH remains negative).
    """
    _reset_x_core()

    log_mock = MagicMock()
    monkeypatch.setattr(x_core.LOGGER, "log", log_mock)

    x_core._CONFIGURATION.log_level = logging.DEBUG

    with EventPublishingContext():
        assert x_core._LAST_EVENT_LOG_LENGTH == -1

    log_mock.assert_not_called()
    assert not x_core._IS_EVENT_IN_PROGRESS


def test_create_base_logging_string_full_logging(monkeypatch):
    _reset_x_core()

    x_core._CONFIGURATION.log_sender_id = True
    x_core._CONFIGURATION.log_event_type = True
    x_core._CONFIGURATION.id_maximum_logging_length = 12
    x_core._EVENT_LENGTH = 26
    x_core._LAST_EVENT_LOG_LENGTH = -1

    event_mock = MagicMock()
    event_mock.sender_id = "SENDER_A"
    event_mock.receiver_id = "RECEIVER_B"
    event_mock.id = "EVENT_ID"
    event_mock.event_type = EventType.DO

    expected = "|  DO  |     SENDER_A ----------- EVENT_ID ----------> RECEIVER_B  "
    result = x_core._create_base_logging_string(event_mock)
    assert result == expected


def test_create_base_logging_string_no_sender(monkeypatch):
    _reset_x_core()

    x_core._CONFIGURATION.log_sender_id = False
    x_core._CONFIGURATION.log_event_type = True
    x_core._CONFIGURATION.id_maximum_logging_length = 12
    x_core._EVENT_LENGTH = 26
    x_core._LAST_EVENT_LOG_LENGTH = -1

    event_mock = MagicMock()
    event_mock.sender_id = "SENDER_A"
    event_mock.receiver_id = "RECEIVER_B"
    event_mock.id = "EVENT_ID"
    event_mock.event_type = EventType.DO

    expected = "|  DO  |  ----------- EVENT_ID ----------> RECEIVER_B  "
    result = x_core._create_base_logging_string(event_mock)
    assert result == expected


def test_create_base_logging_string_no_event_type(monkeypatch):
    _reset_x_core()

    x_core._CONFIGURATION.log_sender_id = True
    x_core._CONFIGURATION.log_event_type = False
    x_core._CONFIGURATION.id_maximum_logging_length = 12
    x_core._EVENT_LENGTH = 26
    x_core._LAST_EVENT_LOG_LENGTH = -1

    event_mock = MagicMock()
    event_mock.sender_id = "SENDER_A"
    event_mock.receiver_id = "RECEIVER_B"
    event_mock.id = "EVENT_ID"
    event_mock.event_type = EventType.DO

    expected = "     SENDER_A ----------- EVENT_ID ----------> RECEIVER_B  "
    result = x_core._create_base_logging_string(event_mock)
    assert result == expected


def test_create_base_logging_string_batched_event(monkeypatch):
    _reset_x_core()

    x_core._CONFIGURATION.log_sender_id = True
    x_core._CONFIGURATION.log_event_type = True
    x_core._CONFIGURATION.id_maximum_logging_length = 12
    x_core._EVENT_LENGTH = 26

    event_mock = MagicMock()
    event_mock.sender_id = "SENDER_A"
    event_mock.receiver_id = "RECEIVER_B"
    event_mock.id = "EVENT_ID"
    event_mock.event_type = EventType.DO

    expected = "|      |     SENDER_A ----------- EVENT_ID ----------> RECEIVER_B  "
    result = x_core._create_base_logging_string(event_mock)
    assert result == expected


def test_create_base_logging_string_non_do_event(monkeypatch):
    _reset_x_core()

    x_core._CONFIGURATION.log_sender_id = True
    x_core._CONFIGURATION.log_event_type = True
    x_core._CONFIGURATION.id_maximum_logging_length = 12
    x_core._EVENT_LENGTH = 26
    x_core._LAST_EVENT_LOG_LENGTH = -1

    event_mock = MagicMock()
    event_mock.sender_id = "SENDER_A"
    event_mock.receiver_id = "RECEIVER_B"
    event_mock.id = "EVENT_ID"
    event_mock.event_type = EventType.UNDO

    expected = "| UNDO |     SENDER_A ----------- EVENT_ID ----------> RECEIVER_B  "
    result = x_core._create_base_logging_string(event_mock)
    assert result == expected


def test_publish_event_in_main_thread(monkeypatch) -> None:
    _reset_x_core()

    monkeypatch.setattr(x_core, "_is_main_thread", lambda: True)
    publish_event_in_main_thread_mock = MagicMock()
    monkeypatch.setattr(x_core, "publish_event_in_main_thread", publish_event_in_main_thread_mock)

    event = TEST_EVENT
    x_core._publish_event(event)

    publish_event_in_main_thread_mock.assert_called_once_with(event)


def test_publish_event_delegates_event_if_not_in_main_thread(monkeypatch) -> None:
    _reset_x_core()

    monkeypatch.setattr(x_core, "_is_main_thread", lambda: False)
    mock_delegator = MagicMock(spec=IXMainThreadDelegator)
    monkeypatch.setattr(x_core, "_MAIN_THREAD_DELEGATOR", mock_delegator)

    event = TEST_EVENT
    x_core._publish_event(event)

    mock_delegator.delegate_event.assert_called_once_with(event)


def test_publish_event_raises_exception_if_not_in_main_thread_and_no_delegator(monkeypatch) -> None:
    _reset_x_core()

    monkeypatch.setattr(x_core, "_is_main_thread", lambda: False)
    monkeypatch.setattr(x_core, "_MAIN_THREAD_DELEGATOR", None)

    with pytest.raises(XNodeException, match=re.escape(
            "Attempted to broadcast events outside of the main thread, with no main thread delegator set.")):
        x_core._publish_event(TEST_EVENT)


def test_publish_event_in_main_thread__no_undo_event(monkeypatch):
    _reset_x_core()

    monkeypatch.setattr(x_core, "EventPublishingContext", contextlib.nullcontext)

    is_main_thread_mock = MagicMock()
    is_main_thread_mock.return_value = True
    monkeypatch.setattr(x_core, "_is_main_thread", is_main_thread_mock)

    log_mock = MagicMock()
    monkeypatch.setattr(x_core, "_log", log_mock)

    execute_event_mock = MagicMock()
    execute_event_mock.return_value = None
    monkeypatch.setattr(x_core, "_execute_event", execute_event_mock)

    publish_undo_redo_counters_mock = MagicMock()
    monkeypatch.setattr(x_core, "_publish_undo_redo_counters", publish_undo_redo_counters_mock)

    event_mock = MagicMock()

    x_core.publish_event_in_main_thread(event_mock)

    log_mock.assert_called_once_with(event_mock)
    execute_event_mock.assert_called_once_with(event_mock)


def test_publish_event_in_main_thread__raise_undo_event_in_broadcast(monkeypatch):
    _reset_x_core()

    monkeypatch.setattr(x_core, "EventPublishingContext", contextlib.nullcontext)

    is_main_thread_mock = MagicMock()
    is_main_thread_mock.return_value = True
    monkeypatch.setattr(x_core, "_is_main_thread", is_main_thread_mock)

    log_mock = MagicMock()
    monkeypatch.setattr(x_core, "_log", log_mock)

    execute_event_mock = MagicMock()
    execute_event_mock.return_value = MagicMock(spec=XEvent)
    monkeypatch.setattr(x_core, "_execute_event", execute_event_mock)

    publish_undo_redo_counters_mock = MagicMock()
    monkeypatch.setattr(x_core, "_publish_undo_redo_counters", publish_undo_redo_counters_mock)

    event_mock = MagicMock()
    event_mock.is_broadcast = True

    with pytest.raises(XNodeException, match=re.escape(
            "Attempted to set an undo event, which is not supported for broadcast events.")):
        x_core.publish_event_in_main_thread(event_mock)


def test_publish_event_in_main_thread__raise_event_not_first_in_batch(monkeypatch):
    _reset_x_core()

    mock_context = MagicMock()
    mock_context.is_first_event_in_batch = False

    mock_context_manager = MagicMock()
    mock_context_manager.return_value.__enter__.return_value = mock_context

    monkeypatch.setattr(x_core, "EventPublishingContext", mock_context_manager)

    is_main_thread_mock = MagicMock()
    is_main_thread_mock.return_value = True
    monkeypatch.setattr(x_core, "_is_main_thread", is_main_thread_mock)

    log_mock = MagicMock()
    monkeypatch.setattr(x_core, "_log", log_mock)

    execute_event_mock = MagicMock()
    execute_event_mock.return_value = MagicMock(spec=XEvent)
    monkeypatch.setattr(x_core, "_execute_event", execute_event_mock)

    publish_undo_redo_counters_mock = MagicMock()
    monkeypatch.setattr(x_core, "_publish_undo_redo_counters", publish_undo_redo_counters_mock)

    event_mock = MagicMock()
    event_mock.is_broadcast = False

    with pytest.raises(XNodeException, match=re.escape(
            "Attempted to set an undo event, but the event is not the first in the batch.")):
        x_core.publish_event_in_main_thread(event_mock)


def test_publish_event_in_main_thread__undo_event_of_do(monkeypatch):
    _reset_x_core()

    mock_context = MagicMock()
    mock_context.is_first_event_in_batch = True

    mock_context_manager = MagicMock()
    mock_context_manager.return_value.__enter__.return_value = mock_context

    monkeypatch.setattr(x_core, "EventPublishingContext", mock_context_manager)

    is_main_thread_mock = MagicMock()
    is_main_thread_mock.return_value = True
    monkeypatch.setattr(x_core, "_is_main_thread", is_main_thread_mock)

    log_mock = MagicMock()
    monkeypatch.setattr(x_core, "_log", log_mock)

    undo_event_mock = MagicMock(spec=XEvent)

    execute_event_mock = MagicMock()
    execute_event_mock.return_value = undo_event_mock
    monkeypatch.setattr(x_core, "_execute_event", execute_event_mock)

    publish_undo_redo_counters_mock = MagicMock()
    monkeypatch.setattr(x_core, "_publish_undo_redo_counters", publish_undo_redo_counters_mock)

    redo_stack_mock = MagicMock()
    monkeypatch.setattr(x_core, "_REDO_STACK", redo_stack_mock)

    append_undo_event_mock = MagicMock()
    monkeypatch.setattr(x_core, "_append_undo_event", append_undo_event_mock)

    event_mock = MagicMock()
    event_mock.is_broadcast = False
    event_mock.event_type = EventType.DO

    x_core.publish_event_in_main_thread(event_mock)

    redo_stack_mock.clear.assert_called_once()
    redo_stack_mock.append.assert_not_called()
    append_undo_event_mock.assert_called_once_with(undo_event_mock)
    publish_undo_redo_counters_mock.assert_called_once()


def test_publish_event_in_main_thread__undo_event_of_undo(monkeypatch):
    _reset_x_core()

    mock_context = MagicMock()
    mock_context.is_first_event_in_batch = True

    mock_context_manager = MagicMock()
    mock_context_manager.return_value.__enter__.return_value = mock_context

    monkeypatch.setattr(x_core, "EventPublishingContext", mock_context_manager)

    is_main_thread_mock = MagicMock()
    is_main_thread_mock.return_value = True
    monkeypatch.setattr(x_core, "_is_main_thread", is_main_thread_mock)

    log_mock = MagicMock()
    monkeypatch.setattr(x_core, "_log", log_mock)

    undo_event_mock = MagicMock(spec=XEvent)

    execute_event_mock = MagicMock()
    execute_event_mock.return_value = undo_event_mock
    monkeypatch.setattr(x_core, "_execute_event", execute_event_mock)

    publish_undo_redo_counters_mock = MagicMock()
    monkeypatch.setattr(x_core, "_publish_undo_redo_counters", publish_undo_redo_counters_mock)

    redo_stack_mock = MagicMock()
    monkeypatch.setattr(x_core, "_REDO_STACK", redo_stack_mock)

    append_undo_event_mock = MagicMock()
    monkeypatch.setattr(x_core, "_append_undo_event", append_undo_event_mock)

    event_mock = MagicMock()
    event_mock.is_broadcast = False
    event_mock.event_type = EventType.UNDO

    x_core.publish_event_in_main_thread(event_mock)

    redo_stack_mock.clear.assert_not_called()
    redo_stack_mock.append.assert_called_once_with(undo_event_mock)
    append_undo_event_mock.assert_not_called()
    publish_undo_redo_counters_mock.assert_called_once()


def test_publish_event_in_main_thread__undo_event_of_redo(monkeypatch):
    _reset_x_core()

    mock_context = MagicMock()
    mock_context.is_first_event_in_batch = True

    mock_context_manager = MagicMock()
    mock_context_manager.return_value.__enter__.return_value = mock_context

    monkeypatch.setattr(x_core, "EventPublishingContext", mock_context_manager)

    is_main_thread_mock = MagicMock()
    is_main_thread_mock.return_value = True
    monkeypatch.setattr(x_core, "_is_main_thread", is_main_thread_mock)

    log_mock = MagicMock()
    monkeypatch.setattr(x_core, "_log", log_mock)

    undo_event_mock = MagicMock(spec=XEvent)

    execute_event_mock = MagicMock()
    execute_event_mock.return_value = undo_event_mock
    monkeypatch.setattr(x_core, "_execute_event", execute_event_mock)

    publish_undo_redo_counters_mock = MagicMock()
    monkeypatch.setattr(x_core, "_publish_undo_redo_counters", publish_undo_redo_counters_mock)

    redo_stack_mock = MagicMock()
    monkeypatch.setattr(x_core, "_REDO_STACK", redo_stack_mock)

    append_undo_event_mock = MagicMock()
    monkeypatch.setattr(x_core, "_append_undo_event", append_undo_event_mock)

    event_mock = MagicMock()
    event_mock.is_broadcast = False
    event_mock.event_type = EventType.REDO

    x_core.publish_event_in_main_thread(event_mock)

    redo_stack_mock.clear.assert_not_called()
    redo_stack_mock.append.assert_not_called()
    append_undo_event_mock.assert_called_once_with(undo_event_mock)
    publish_undo_redo_counters_mock.assert_called_once()


def test_is_main_thread__execution(monkeypatch):
    thread_mock_instance = MagicMock()

    threading_mock = MagicMock()
    threading_mock.current_thread.return_value = thread_mock_instance
    threading_mock.main_thread.return_value = thread_mock_instance
    monkeypatch.setattr(x_core, "threading", threading_mock)

    assert x_core._is_main_thread()
