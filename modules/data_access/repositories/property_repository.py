#!/usr/bin/env python3
from typing import List
from abc import ABC, abstractmethod

from data_access.property import Property


class PropertyRepository(ABC):
    """Class to represent a property repository."""

    @abstractmethod
    def get_named_location_properties(self, named_location_id: int) -> List[Property]:
        pass
