"""
xnodes: Exchange nodes framework
        Simplistic event framework which enables unrelated nodes to exchange information, alter each other states and
        provides the possibility to undo made changes.

Author: Ralph Neumann (@newmra)
"""
import enum
from dataclasses import dataclass
from typing import Dict

from xnodes import x_event_description


class EventType(enum.Enum):
    """
    Enum for the event type.
    """
    DO = "DO"
    UNDO = "UNDO"
    REDO = "REDO"


@dataclass(frozen=True)
class XEvent:
    """
    Complete event which is sent from one node to another node.
    """
    id: str
    event_description: x_event_description.XEventDescription
    sender_id: str
    receiver_id: str
    parameters: Dict[str, object]
    event_type: EventType = EventType.DO
    is_broadcast: bool = False
