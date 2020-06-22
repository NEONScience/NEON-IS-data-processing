#!/usr/bin/env python3
from geojson import FeatureCollection
from typing import Iterator, Optional
from abc import ABC, abstractmethod


class NamedLocationRepository(ABC):
    """Class to represent a named location repository."""

    @abstractmethod
    def get_by_type(self, type_name: str, cutoff_date=None) -> Iterator[FeatureCollection]:
        pass

    @abstractmethod
    def get_asset_history(self, asset_id: int) -> FeatureCollection:
        pass

    @abstractmethod
    def get_schema_name(self, named_location_name: str) -> Optional[str]:
        pass
