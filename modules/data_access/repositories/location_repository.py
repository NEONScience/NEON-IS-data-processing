#!/usr/bin/env python3
from abc import ABC, abstractmethod

from geojson import FeatureCollection


class LocationRepository(ABC):
    """Class to represent a location repository backed by a database."""

    @abstractmethod
    def get_all(self, named_location_id: int) -> FeatureCollection:
        pass
