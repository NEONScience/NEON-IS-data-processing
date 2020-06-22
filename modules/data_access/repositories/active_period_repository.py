#!/usr/bin/env python3
from typing import List
from abc import ABC, abstractmethod

from data_access.active_period import ActivePeriod


class ActivePeriodRepository(ABC):
    """Class to represent an active period repository for named locations."""

    @abstractmethod
    def get_active_periods(self, named_location_id: int, cutoff_date=None) -> List[ActivePeriod]:
        pass
