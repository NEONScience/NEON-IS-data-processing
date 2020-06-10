#!/usr/bin/env python3
from collections import Generator

import geojson


class NamedLocationRepository:

    def __init__(self, repository):
        self._repository = repository

    def __getattr__(self, attribute):
        return getattr(self._repository, attribute)

    def get_by_type(self, type_name: str, cutoff_date=None) -> Generator[geojson.FeatureCollection]:
        return self._repository.get_by_type(type_name, cutoff_date)

    def get_asset_history(self, asset_id: int) -> geojson.FeatureCollection:
        return self._repository.get_asset_history(asset_id)

    def get_site(self, named_location_id: int) -> str:
        return self._repository.get_site(named_location_id)

    def get_schema_name(self, named_location_name: str) -> str:
        return self._repository.get_schema_name(named_location_name)
