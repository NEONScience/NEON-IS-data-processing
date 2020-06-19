#!/usr/bin/env python3
from typing import List
from abc import ABC, abstractmethod


class ThresholdContextRepository(ABC):
    """Class to represent a threshold context repository."""

    @abstractmethod
    def get_context(self, condition_uuid: str) -> List[str]:
        pass
