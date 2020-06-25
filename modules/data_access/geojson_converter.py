from typing import List

from geojson import Feature, FeatureCollection

from common.date_formatter import convert
from data_access.named_location import NamedLocation
from data_access.asset_location import AssetLocation
from data_access.active_period import ActivePeriod


def convert_asset_location(location: AssetLocation) -> Feature:
    install_date = location.install_date
    remove_date = location.remove_date
    transaction_date = location.transaction_date
    if install_date is not None:
        install_date = convert(install_date)
    if location.remove_date is not None:
        remove_date = convert(location.remove_date)
    if location.transaction_date is not None:
        transaction_date = convert(location.transaction_date)
    feature_properties = {'name': location.name,
                          'site': location.site,
                          'install_date': install_date,
                          'remove_date': remove_date,
                          'transaction_date': transaction_date,
                          'context': location.context}
    for prop in location.properties:
        feature_properties[prop.name] = prop.value
    feature_properties['locations'] = location.locations
    return Feature(properties=feature_properties)


def convert_named_location(location: NamedLocation) -> FeatureCollection:
    active_periods = convert_active_periods(location.active_periods)
    properties = {'name': location.name,
                  'type': location.type,
                  'description': location.description,
                  'site': location.site,
                  'context': location.context,
                  'active_periods': active_periods}
    for prop in location.properties:
        properties[prop.name] = prop.value
    return FeatureCollection([Feature(properties=properties)])


def convert_active_periods(active_periods: List[ActivePeriod]) -> List[dict]:
    periods = []
    for period in active_periods:
        if period.end_date is not None:
            periods.append({'start_date': period.start_date, 'end_date': period.end_date})
        else:
            periods.append({'start_date': period.start_date})
    return periods
