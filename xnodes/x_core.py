"""
xnodes: Exchange nodes framework
        Simplistic event framework which enables unrelated nodes to exchange information, alter each other states and
        provides the possibility to undo made changes.

Author: Ralph Neumann (@newmra)
"""
import inspect
import logging
import threading
from collections import defaultdict
from copy import copy
from types import GeneratorType
from typing import Callable, Optional, Dict, List, Set, Tuple, Iterable, Any

from xnodes.x_core_configuration import XCoreConfiguration
from xnodes.x_event import XEvent, EventType
from xnodes.x_event_description import XEventDescription, XEventParameter
from xnodes.x_event_listener import X_EVENT_LISTENER_FLAG, APPEND_SENDER_ID_FLAG
from xnodes.x_node_exception import XNodeException

LOGGER = logging.getLogger(__name__)


class IMainThreadDelegator:
    """
    Interface for main thread delegators.
    """

    def delegate_events(self, events: List[XEvent]) -> None:
        """
        Delegate a list of events to the main thread.
        :param events: Events to delegate.
        :return: None
        """
        raise NotImplementedError()

    def _delegate_events_to_main_thread(self, events: List[XEvent]) -> None:
        """
        Delegate a list of events to the main thread.
        :param events: Events to delegate.
        :return: None
        """
        raise NotImplementedError()


X_CORE_NODE_ID = "X_CORE"

X_CORE_START = "X_CORE_START"
X_MAP_UNDO_REDO_COUNTERS = "X_MAP_UNDO_REDO_COUNTERS"
X_CLEAR_UNDO_REDO_EVENTS = "X_CLEAR_UNDO_REDO_EVENTS"

_SENDER_ID_PARAMETER_NAME = "sender_id"

_UNDO_STACK: List[List[XEvent]] = []
_REDO_STACK: List[List[XEvent]] = []

_NODE_IDS = {X_CORE_NODE_ID}

_EVENT_SUBSCRIPTIONS: Dict[str, Set[str]] = defaultdict(set)
_EVENT_SUBSCRIPTIONS[X_CLEAR_UNDO_REDO_EVENTS].add(X_CORE_NODE_ID)

# pylint: disable = unnecessary-lambda
# Functions are declared later, so lambda is necessary.
_EVENT_HANDLERS: Dict[Tuple[str, str], Callable] = {
    (X_CLEAR_UNDO_REDO_EVENTS, X_CORE_NODE_ID): lambda: _clear_undo_redo_stacks()
}
# pylint: enable = unnecessary-lambda

_EVENT_DESCRIPTIONS: Dict[str, XEventDescription] = {
    X_CORE_START: XEventDescription(set(), logging.INFO),
    X_MAP_UNDO_REDO_COUNTERS: XEventDescription(
        {
            XEventParameter("undo_counter", int, "Number of undo events."),
            XEventParameter("redo_counter", int, "Number of redo events.")
        }, logging.INFO),
    X_CLEAR_UNDO_REDO_EVENTS: XEventDescription(set(), logging.INFO)
}

# Event length is the length of the biggest event ID name.
_MINIMUM_ID_MAXIMUM_LOGGING_LENGTH = 10
_EVENT_LENGTH = 24
_IS_EVENT_IN_PROGRESS = False
_CONFIGURATION = XCoreConfiguration()
_MAIN_THREAD_DELEGATOR: IMainThreadDelegator or None = None


def _get_parameter_names(event_description: XEventDescription) -> Set[str]:
    """
    Get all parameter names of the given event description.
    :param event_description: Event description to get the parameter names of.
    :return: All event parameter names of the given event description.
    """
    event_parameters = set()
    for parameter in event_description.parameters:
        event_parameters.add(parameter.name)
    return event_parameters


def _build_event(
        event_id: str,
        sender_id: str,
        receiver_id: str,
        parameters: Dict[str, object],
        event_type: EventType
) -> XEvent:
    """
    Build an event with the given information.
    :param event_id: ID of the event.
    :param sender_id: ID of the sender node.
    :param receiver_id: ID of the receiver node.
    :param parameters: Parameters of the event.
    :param event_type: Type of the event.
    :return: Constructed event.
    """
    if event_id not in _EVENT_DESCRIPTIONS:
        raise XNodeException(f"Attempted to create an unknown event '{event_id}'.")

    event_description = _EVENT_DESCRIPTIONS[event_id]

    event_parameters = _get_parameter_names(event_description)
    if event_parameters != set(parameters):
        event_parameters_string = ", ".join([f"'{parameter}'" for parameter in event_parameters])
        provided_parameters_string = ", ".join([f"'{parameter}'" for parameter in parameters])

        raise XNodeException(
            f"Event '{event_id}' cannot be constructed, event requires: [{event_parameters_string}], "
            f"provided are: [{provided_parameters_string}].")

    return XEvent(event_id, event_description, sender_id, receiver_id, parameters, event_type)


def register_event(event_id: str, parameters: Set[XEventParameter], log_level: int = logging.INFO) -> None:
    """
    Register a new event in the core, event ID cannot be registered yet. Once an event is registered it cannot
    be removed.
    :param event_id: ID of the event.
    :param parameters: Parameters of the event.
    :param log_level: Log level of the event.
    :return: None
    """
    global _EVENT_LENGTH

    if event_id in _EVENT_DESCRIPTIONS:
        raise XNodeException(f"Attempted to register event '{event_id}' twice.")

    if not isinstance(log_level, int):
        raise XNodeException(
            f"Attempted to register event '{event_id}', but the log_level is not of type 'int'.")

    parameter_names = set()

    for i, parameter in enumerate(parameters):
        if not isinstance(parameter, XEventParameter):
            raise XNodeException(
                f"Attempted to register event '{event_id}', but parameter {i} is not of type "
                f"'{XEventParameter.__name__}'.")

        if not isinstance(parameter.name, str):
            raise XNodeException(
                f"Attempted to register event '{event_id}', but parameter {i} has an invalid name, has to be "
                f"of type 'str'.")

        if not isinstance(parameter.type, (type, type(None))):
            raise XNodeException(
                f"Attempted to register event '{event_id}', but parameter {i} has an invalid type, has to be "
                f"of type 'type' or None.")

        if not isinstance(parameter.description, str):
            raise XNodeException(
                f"Attempted to register event '{event_id}', but parameter {i} has an invalid description, has "
                f"to be of type 'str'.")

        if parameter.name in parameter_names:
            raise XNodeException(
                f"Attempted to register event '{event_id}', but parameter {parameter.name} is configured twice."
            )

        if parameter.name == _SENDER_ID_PARAMETER_NAME:
            raise XNodeException(
                f"Attempted to register event '{event_id}', but parameter {parameter.name} is a reserved name."
            )

        parameter_names.add(parameter.name)

    _EVENT_DESCRIPTIONS[event_id] = XEventDescription(parameters, log_level)
    _EVENT_LENGTH = max(len(event_id) for event_id in _EVENT_DESCRIPTIONS)


def register_node(node_id: str, node: object) -> None:
    """
    Register a new node, node ID cannot be registered yet.
    :param node_id: ID of the node.
    :param node: Node to register.
    :return: None
    """
    if node_id in _NODE_IDS:
        raise XNodeException(f"Attempted to register node '{node_id}', but a node with that ID is already registered.")

    for _, node_method in inspect.getmembers(node, predicate=inspect.ismethod):
        if not hasattr(node_method, X_EVENT_LISTENER_FLAG):
            continue

        event_id = getattr(node_method, X_EVENT_LISTENER_FLAG)
        if (event_description := _EVENT_DESCRIPTIONS.get(event_id)) is None:
            raise XNodeException(f"Node '{node_id}' handles event '{event_id}', but the event is not registered.")

        node_method_arguments = inspect.getfullargspec(node_method).args
        node_method_event_parameters = copy(node_method_arguments)

        # Remove the 'self' reference.
        if "self" in node_method_event_parameters:
            node_method_event_parameters.remove("self")
        if _SENDER_ID_PARAMETER_NAME in node_method_event_parameters:
            node_method_event_parameters.remove(_SENDER_ID_PARAMETER_NAME)

        if hasattr(node_method, APPEND_SENDER_ID_FLAG) and getattr(node_method, APPEND_SENDER_ID_FLAG) is True:
            if _SENDER_ID_PARAMETER_NAME not in node_method_arguments:
                raise XNodeException(
                    f"Node '{node_id}' handles event '{event_id}', but the event listener does not "
                    "request the sender id in its decorator.")

        event_parameters = _get_parameter_names(event_description)
        if event_parameters != set(node_method_event_parameters):
            event_parameters_string = ", ".join([f"'{parameter}'" for parameter in event_parameters])
            handler_parameters_string = ", ".join([f"'{parameter}'" for parameter in node_method_event_parameters])

            raise XNodeException(
                f"Node '{node_id}' handles event '{event_id}', but the parameters do not match. "
                f"Event requires: [{event_parameters_string}], handler provides: [{handler_parameters_string}].")

        _EVENT_SUBSCRIPTIONS[event_id].add(node_id)
        _EVENT_HANDLERS[(event_id, node_id)] = node_method

    _NODE_IDS.add(node_id)


def unregister_node(node_id: str) -> None:
    """
    Unregister the node with the given ID. The node will no longer receive events or is allowed to publish
    and broadcast events.
    :param node_id: ID of the node to unregister.
    :return: None
    """
    if node_id not in _NODE_IDS:
        raise XNodeException(f"Attempted to unregister node '{node_id}', but no node with that ID is registered.")

    for event_id, subscribers in _EVENT_SUBSCRIPTIONS.items():
        if node_id not in subscribers:
            continue

        subscribers.remove(node_id)
        _EVENT_HANDLERS.pop((event_id, node_id))

    _NODE_IDS.remove(node_id)


def start(
        x_core_configuration: Optional[XCoreConfiguration] = None,
        main_thread_delegator: Optional[IMainThreadDelegator] = None
) -> None:
    """
    Start the core and send the X_CORE_START event to all nodes which subscribed to it.
    :param x_core_configuration: Core configuration.
    :param main_thread_delegator: Main thread delegator.
    :return: None
    """
    global _CONFIGURATION
    global _MAIN_THREAD_DELEGATOR

    if isinstance(x_core_configuration, XCoreConfiguration):
        _CONFIGURATION = x_core_configuration

    if _CONFIGURATION.id_maximum_logging_length < _MINIMUM_ID_MAXIMUM_LOGGING_LENGTH:
        raise XNodeException(f"Invalid configuration: 'id_maximum_logging_length' has to be "
                             f"greater or equal to {_MINIMUM_ID_MAXIMUM_LOGGING_LENGTH}.")

    if main_thread_delegator is not None and not isinstance(main_thread_delegator, IMainThreadDelegator):
        raise XNodeException(f"Main thread delegator has to be of type '{IMainThreadDelegator.__name__}'.")
    _MAIN_THREAD_DELEGATOR = main_thread_delegator

    LOGGER.setLevel(_CONFIGURATION.log_level)

    broadcast(X_CORE_START, X_CORE_NODE_ID, {})


def publish(event_id: str, sender_id: str, receiver_id: str, parameters: Dict[str, object]) -> None:
    """
    Publish a directed event to another node. The event has to be registered and the receiver node ID has to be
    registered. Additionally, the parameters have to match the parameters of the registered event.
    :param event_id: ID of the event to publish.
    :param sender_id: ID of the node which sent the event.
    :param receiver_id: ID of the node which shall receive the event.
    :param parameters: Parameters of the event. Have to match exactly the parameters of the event.
    :return: None
    """
    base_error_message = f"Node '{sender_id}' attempted to publish event '{event_id}' to node '{receiver_id}'"

    if event_id not in _EVENT_DESCRIPTIONS:
        raise XNodeException(f"{base_error_message}, but the event is not registered.")

    if sender_id not in _NODE_IDS:
        raise XNodeException(f"{base_error_message}, but the sender node is not registered.")

    if receiver_id not in _NODE_IDS:
        raise XNodeException(f"{base_error_message}, but the receiver node is not registered.")

    if (event_id, receiver_id) not in _EVENT_HANDLERS:
        raise XNodeException(f"{base_error_message}, but receiver '{receiver_id}' is not subscribed to event "
                             f"'{event_id}'.")

    publish_events([_build_event(event_id, sender_id, receiver_id, parameters, EventType.DO)])


def broadcast(event_id: str, sender_id: str, parameters: Dict[str, object]) -> None:
    """
    Broadcast an event to all nodes which are subscribed to the event. The event has to be registered and the
    receiver node ID has to be registered. Additionally, the parameters have to match the parameters of the
    registered event.
    :param event_id: ID of the event to broadcast.
    :param sender_id: ID of the node which sent the event.
    :param parameters: Parameters of the event. Have to match exactly the parameters of the event.
    :return: None
    """
    base_error_message = f"Node '{sender_id}' attempted to broadcast event '{event_id}'"

    if event_id not in _EVENT_DESCRIPTIONS:
        raise XNodeException(f"{base_error_message}, but the event is not registered.")

    if sender_id not in _NODE_IDS:
        raise XNodeException(f"{base_error_message}, but the sender node is not registered.")

    events = [
        _build_event(event_id, sender_id, handler_id, parameters, EventType.DO)
        for handler_id in _EVENT_SUBSCRIPTIONS[event_id]
    ]

    _log(_build_event(event_id, sender_id, "BROADCAST", parameters, EventType.DO))

    if not events:
        return

    publish_events(events)


def add_undo_event(event_id: str, receiver_id: str, parameters: Dict[str, object] or None = None) -> None:
    """
    Add a new undo event to the stack and clear the redo stack. This function can be used to add an undo event
    to changes which were not induced by an event. Has to be used with care, because it can alter the event flow.
    :param event_id: ID of the event to publish.
    :param receiver_id: ID of the node which shall receive the event.
    :param parameters: Parameters of the event. Have to match exactly the parameters of the event.
    :return: None
    """
    base_error_message = f"Attempted to add an undo event '{event_id}' addressed to node '{receiver_id}'"

    if event_id not in _EVENT_DESCRIPTIONS:
        raise XNodeException(f"{base_error_message}, but the event is not registered.")

    if receiver_id not in _NODE_IDS:
        raise XNodeException(f"{base_error_message}, but the receiver node is not registered.")

    if (event_id, receiver_id) not in _EVENT_HANDLERS:
        raise XNodeException(f"{base_error_message}, but receiver '{receiver_id}' is not subscribed to event "
                             f"'{event_id}'.")

    parameters = parameters or {}

    _append_undo_event(_build_event(event_id, X_CORE_NODE_ID, receiver_id, parameters, EventType.UNDO))
    _REDO_STACK.clear()
    _publish_undo_redo_counters()


def _log(event: XEvent) -> None:
    """
    Log an event to the console.
    :param event: Event to log.
    :return: None
    """
    base_string = _create_base_logging_string(event)
    parameters_logging_string = _create_parameters_logging_string(event)

    LOGGER.log(event.event_description.log_level, f"{base_string}{parameters_logging_string}".rstrip())


def _create_base_logging_string(event: XEvent) -> str:
    """
    Create the base logging string for the given event.
    :param event: Event to create the base logging string for.
    :return: Base logging string of the passed event.
    """
    if _CONFIGURATION.log_sender_id:
        from_str = (" " * (_CONFIGURATION.id_maximum_logging_length - len(event.sender_id)) + event.sender_id)
    else:
        from_str = ""

    to_str = event.receiver_id + " " * (_CONFIGURATION.id_maximum_logging_length - len(event.receiver_id))

    if _CONFIGURATION.log_event_type:
        event_type_str = str(event.event_type.value)
        if event.event_type == EventType.DO:
            event_type_str = f" {event_type_str} "

        event_type_str = f" | {event_type_str} |"
    else:
        event_type_str = ""

    remaining_dashes = _EVENT_LENGTH - len(event.id)

    event_str = "--"
    event_str += "-" * (remaining_dashes // 2)
    event_str += f" {event.id} "
    event_str += "-" * (remaining_dashes - remaining_dashes // 2)
    event_str += "->"

    return f"{event_type_str} {from_str} {event_str} {to_str}"


def _create_parameters_logging_string(event: XEvent) -> str:
    """
    Create the parameters logging string for the given event.
    :param event: Event to create the parameters logging string for.
    :return: Parameters logging string of the passed event.
    """
    if not _CONFIGURATION.log_event_parameters or not event.event_description.parameters:
        return ""

    part_strings = []

    for parameter in event.event_description.parameters:
        type_info = ""
        if _CONFIGURATION.log_parameter_type_info and isinstance(parameter.type, type):
            type_info = f" ({parameter.type.__name__} / {type(event.parameters[parameter.name]).__name__})"

        part_strings.append(f"{parameter.name}{type_info}: '{event.parameters[parameter.name]}'")

    return " | " + " | ".join(part_strings)


class EventPublishingContext:
    """
    Context for publishing events.
    """

    def __init__(self, events: List[XEvent]):
        """
        Init of EventPublishingContext.
        :param events: Events which are published.
        """
        global _IS_EVENT_IN_PROGRESS

        self._is_first_event_in_batch = not _IS_EVENT_IN_PROGRESS
        _IS_EVENT_IN_PROGRESS = True

        self._events = events

    @property
    def is_first_event_in_batch(self) -> bool:
        """
        Check if this is the first event in the batch.
        :return: True if this is the first event in the batch, False otherwise.
        """
        return self._is_first_event_in_batch

    def __enter__(self):
        """
        Enter the context. If no event is currently published, an empty line is logged to separate the event stack from
        the remaining log.
        :return: Self.
        """
        if not self._is_first_event_in_batch:
            return self

        for event in self._events:
            if event.event_description.log_level >= _CONFIGURATION.log_level:
                LOGGER.log(event.event_description.log_level, "***")
                break

        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        """
        Exit the context.
        :param exc_type: Exception type.
        :param exc_val: Exception value.
        :param exc_tb: Exception traceback.
        :return: None
        """
        global _IS_EVENT_IN_PROGRESS
        if self._is_first_event_in_batch:
            _IS_EVENT_IN_PROGRESS = False


def publish_events(events: List[XEvent]) -> None:
    """
    Publish a list of events and return the undo events.
    :param events: Events to publish.
    :return: None
    """
    if _is_main_thread():
        publish_events_in_main_thread(events)
    elif isinstance(_MAIN_THREAD_DELEGATOR, IMainThreadDelegator):
        _MAIN_THREAD_DELEGATOR.delegate_events(events)
    else:
        raise XNodeException(
            "Attempted to broadcast events outside of the main thread, with no main thread delegator set.")


def publish_events_in_main_thread(events: List[XEvent]) -> None:
    """
    Publish a list of events in the main thread.
    :param events: Events to publish.
    :return: None
    """
    if not _is_main_thread():
        raise XNodeException("Attempted to publish events outside of the main thread.")

    with EventPublishingContext(events) as context:
        for i, event in enumerate(events):
            _log(event)

            undo_event_generator = _execute_event(event)
            extracted_undo_event = _extract_undo_event(undo_event_generator, event.receiver_id, event.event_type)
            if not isinstance(extracted_undo_event, XEvent):
                continue

            if i != 0 or not context.is_first_event_in_batch:
                raise XNodeException("Undo event provided for an event which was not the first in the batch.")

            if event.event_type == EventType.DO:
                _REDO_STACK.clear()
                _append_undo_event(extracted_undo_event)
            elif event.event_type == EventType.UNDO:
                _REDO_STACK.append([extracted_undo_event])
            elif event.event_type == EventType.REDO:
                _append_undo_event(extracted_undo_event)

            further_undo_event = _extract_undo_event(undo_event_generator, event.receiver_id, event.event_type)

            if further_undo_event is not None:
                raise XNodeException(f"An event can only return a single undo event, but a second one was returned.")

            _publish_undo_redo_counters()


def _execute_event(event: XEvent) -> GeneratorType:
    """
    Execute the given event and call the receiver nodes event handler.
    :param event: Event to execute.
    :return: An undo event generator which is provided by the event handler of the receiver node.
    """
    if (event.id, event.receiver_id) not in _EVENT_HANDLERS:
        raise XNodeException(f"Attempted to send event with ID '{event.id}' to node "
                             f"'{event.receiver_id}', but the node is not subscribed to that event.")

    parameter_description = event.event_description.parameters

    event_handler = _EVENT_HANDLERS[(event.id, event.receiver_id)]
    parameters = {parameter.name: event.parameters[parameter.name] for parameter in parameter_description}

    if hasattr(event_handler, APPEND_SENDER_ID_FLAG) and getattr(event_handler, APPEND_SENDER_ID_FLAG) is True:
        parameters[_SENDER_ID_PARAMETER_NAME] = event.sender_id

    return event_handler(**parameters)


def _extract_undo_event(undo_event_iterable: Any, receiver_id: str, original_event_type: EventType) -> XEvent or None:
    """
    Extract the undo events from the given undo event iterable.
    :param undo_event_iterable: Undo event iterable to extract the undo events from.
    :param receiver_id: ID of the receiver node.
    :param original_event_type: Type of the original event.
    :return: Extracted undo event or None if no undo event is returned.
    """
    if not isinstance(undo_event_iterable, Iterable):
        return []

    for undo_event in undo_event_iterable:
        if not isinstance(undo_event, tuple) or len(undo_event) != 2:
            raise XNodeException("Undo event has to be a tuple consisting of the event and the parameters.")

        undo_event_id, undo_event_parameters = undo_event

        if not isinstance(undo_event_parameters, dict):
            raise XNodeException(f"Undo event parameters has an invalid type, should be dict, is: "
                                 f"'{type(undo_event_parameters).__name__}'.")

        if original_event_type == EventType.DO or original_event_type == EventType.REDO:
            undo_event_type = EventType.UNDO
        elif original_event_type == EventType.UNDO:
            undo_event_type = EventType.REDO
        else:
            raise XNodeException(
                f"Original event type '{original_event_type}' is not supported, expected 'DO' or 'UNDO'.")

        return _build_event(undo_event_id, receiver_id, receiver_id, undo_event_parameters, undo_event_type)

    return None


def undo() -> None:
    """
    Publish the last undo events and save the redo events to the redo stack.
    :return: None
    """
    if not _UNDO_STACK:
        return

    publish_events(_UNDO_STACK.pop(-1))


def redo() -> None:
    """
    Perform the last redo events and save the undo events to the undo stack.
    :return: None
    """
    if not _REDO_STACK:
        return

    publish_events(_REDO_STACK.pop(-1))


def _append_undo_event(undo_event: XEvent) -> None:
    """
    Append a new undo event.
    :param undo_event: Undo event to append.
    :return: None
    """
    _UNDO_STACK.append([undo_event])
    if _CONFIGURATION.maximum_undo_events < 0:
        return

    if len(_UNDO_STACK) > _CONFIGURATION.maximum_undo_events:
        _UNDO_STACK.pop(0)


def _clear_undo_redo_stacks() -> None:
    """
    Clear the undo and redo stacks.
    :return: None
    """
    _UNDO_STACK.clear()
    _REDO_STACK.clear()

    _publish_undo_redo_counters()


def _publish_undo_redo_counters() -> None:
    """
    Publish the undo and redo counters.
    :return: None
    """
    broadcast(X_MAP_UNDO_REDO_COUNTERS, X_CORE_NODE_ID, {
        "undo_counter": len(_UNDO_STACK),
        "redo_counter": len(_REDO_STACK)
    })


def _is_main_thread() -> bool:
    """
    Check if the current thread is the main thread.
    :return: True if the current thread is the main thread, False otherwise.
    """
    return threading.current_thread() is threading.main_thread()
