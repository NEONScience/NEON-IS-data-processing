#!/usr/bin/env python3
from abc import ABC, abstractmethod

from data_access.named_location_parent import NamedLocationParent


class NamedLocationParentRepository(ABC):
    """Class to represent a named location parent repository."""

    @abstractmethod
    def get_site(self, named_location_id: int) -> NamedLocationParent:
        pass
