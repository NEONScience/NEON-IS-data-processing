from typing import List

from geojson import Feature, FeatureCollection

import common.date_formatter as date_formatter
from data_access.types.named_location import NamedLocation
from data_access.types.asset_location import AssetLocation
from data_access.types.active_period import ActivePeriod
from data_access.types.group import Group


def convert_asset_location(location: AssetLocation) -> Feature:
    install_date = location.install_date
    remove_date = location.remove_date
    if install_date is not None:
        install_date = date_formatter.to_string(install_date)
    if location.remove_date is not None:
        remove_date = date_formatter.to_string(location.remove_date)
    feature_properties = dict(name=location.name,
                              domain=location.domain,
                              site=location.site,
                              install_date=install_date,
                              remove_date=remove_date,
                              context=location.context,
                              site_location=location.site_location,
                              locations=location.locations)
    feature = Feature(properties=feature_properties)
    for p in location.properties:
        feature[p.name] = p.value
    return feature


def convert_named_location(location: NamedLocation) -> FeatureCollection:
    active_periods = convert_active_periods(location.active_periods)
    properties = dict(name=location.name,
                      type=location.type,
                      description=location.description,
                      domain=location.domain,
                      site=location.site,
                      context=location.context,
                      active_periods=active_periods)
    feature = Feature(properties=properties)
    for p in location.properties:
        feature[p.name] = p.value
    return FeatureCollection([feature])


def convert_group(group: Group) -> FeatureCollection:
    g = 0
    g_len = len(group) - 1
    feature_list = []
    while g < g_len:
        active_periods = convert_active_periods(group[g].active_periods)
        properties = dict(name=group[g].name,
                  group=group[g].group,
                  active_periods=active_periods,
                  data_product_ID=group[g].data_product_ID)
        feature = Feature(properties=properties)
        for p in group[g].properties:
            feature[p.name] = p.value
        feature_list.append(feature)
        g = g + 1
    return FeatureCollection(feature_list)


def convert_active_periods(active_periods: List[ActivePeriod]) -> List[dict]:
    periods = []
    for period in active_periods:
        start_date = period.start_date
        end_date = period.end_date
        if start_date is not None:
            formatted_start_date = date_formatter.to_string(start_date)
            if end_date is not None:
                formatted_end_date = date_formatter.to_string(end_date)
                periods.append(dict(start_date=formatted_start_date, end_date=formatted_end_date))
            else:
                periods.append(dict(start_date=formatted_start_date, end_date=end_date))
        elif end_date is not None:
            formatted_end_date = date_formatter.to_string(end_date)
            periods.append(dict(start_date=start_date, end_date=formatted_end_date))
        else:
            periods.append(dict(start_date=start_date, end_date=end_date))
    return periods
