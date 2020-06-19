#!/usr/bin/env python3
from abc import ABC, abstractmethod
from typing import List, Union, Dict


class ThresholdRepository(ABC):
    """Class representing a threshold repository."""

    @abstractmethod
    def get_thresholds(self) -> List[Dict[str, Union[str, List[str], None]]]:
        pass
