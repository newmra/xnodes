"""
    Copyright (c) 2024 - All rights reserved
    Unauthorized copying of this file, via any medium is strictly prohibited

    File:       i_x_main_thread_delegator.py
    Created on: 03.08.2025
    Author:     Ralph Neumann
"""
from xnodes import XEvent


class IXMainThreadDelegator:
    """
    Interface for main thread delegators.
    """

    def delegate_event(self, event: XEvent) -> None:
        """
        Delegate an event to the main thread.
        :return: None
        """
        raise NotImplementedError()
