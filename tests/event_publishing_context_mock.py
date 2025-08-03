"""
    Copyright (c) 2024 - All rights reserved
    Unauthorized copying of this file, via any medium is strictly prohibited

    File:       event_publishing_context_mock.py
    Created on: 03.08.2025
    Author:     Ralph Neumann
"""


class EventPublishingContextMock:

    def __init__(self, is_first_event_in_batch: bool = True):
        self._is_first_event_in_batch = is_first_event_in_batch

    def __enter__(self, event):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        print(exc_type)
        pass
