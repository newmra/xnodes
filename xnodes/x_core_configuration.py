"""
xnodes: Exchange nodes framework
        Simplistic event framework which enables unrelated nodes to exchange information, alter each other states and
        provides the possibility to undo made changes.

Author: Ralph Neumann (@newmra)
"""

import logging
from dataclasses import dataclass


@dataclass
class XCoreConfiguration:
    """
    Configuration of the XCore.
    """
    log_level: int = logging.INFO
    log_event_type: bool = False
    log_sender_id: bool = True
    log_event_parameters: bool = True
    log_parameter_type_info: bool = False
    id_maximum_logging_length: int = 40
    maximum_undo_events: int = 1000
