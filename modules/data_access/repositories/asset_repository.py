#!/usr/bin/env python3
from typing import List
from abc import ABC, abstractmethod

from data_access.asset import Asset


class AssetRepository(ABC):
    """Class to represent an asset repository."""

    @abstractmethod
    def get_all(self) -> List[Asset]:
        pass
