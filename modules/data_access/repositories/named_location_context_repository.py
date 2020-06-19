#!/usr/bin/env python3
from typing import List
from abc import ABC, abstractmethod


class NamedLocationContextRepository(ABC):
    """Class to represent a named location context repository."""

    @abstractmethod
    def get_context(self, named_location_id: int) -> List[str]:
        pass
