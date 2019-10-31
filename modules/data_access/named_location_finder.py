from contextlib import closing

from geojson import Feature, FeatureCollection
import structlog

import data_access.location_finder as location_finder
import lib.date_formatter as date_formatter

log = structlog.get_logger()


def get_active_periods(connection, named_location_id):
    """
    Get the active time periods for a named location.
    :param connection: A database connection
    :param named_location_id: The ID to search on.
    :return: Dictionary of active periods.
    """
    sql = '''
        select start_date, end_date from active_period where named_location_id = :id
    '''
    with closing(connection.cursor()) as cursor:
        cursor.prepare(sql)
        rows = cursor.execute(None, id=named_location_id)
        periods = []
        for row in rows:
            start_date = date_formatter.format(row[0])
            end_date = date_formatter.format(row[1])
            periods.append({'start_date': start_date, 'end_date': end_date})
        return periods


def get_type_context(connection, type_name, context):
    """
    Get named locations by type.
    :param connection: A database connection.
    :param type_name: The named location type.
    :param context: The named location context.
    :return: Geojson FeatureCollection of locations.
    """
    sql = '''
            select
                nam_locn.nam_locn_id,
                nam_locn.nam_locn_name,
                nam_locn.nam_locn_desc,
                type.type_name
            from
                nam_locn, type, nam_locn_context
            where
                type.type_id = nam_locn.type_id
            and
                type.type_name = :type_name
            and 
                nam_locn.nam_locn_id = nam_locn_context.nam_locn_id 
            and 
                nam_locn_context.context_code = :context
            /* fetch first 1 rows only */
        '''
    with closing(connection.cursor()) as cursor:
        cursor.prepare(sql)
        rows = cursor.execute(None, type_name=type_name, context=context)
        named_locations = []
        for row in rows:
            features = []
            named_location_id = row[0]
            name = row[1]
            description = row[2]
            log.debug(f'named_location name: {name} description {description}')

            location_properties = get_properties(connection, named_location_id)
            parents = get_parents(connection, named_location_id)
            context = get_context(connection, named_location_id)
            active_periods = get_active_periods(connection, named_location_id)

            # The parent value contains only site
            site = parents[0]['name']

            feature = Feature(properties={'name': name,
                                          'type': type_name,
                                          'description': description,
                                          'site': site,
                                          'context': context,
                                          'active-periods': active_periods})

            for prop in location_properties:
                non_null_field = 'string_value'
                if prop['number_value'] is not None:
                    non_null_field = 'number_value'
                if prop['date_value'] is not None:
                    non_null_field = 'date_value'
                feature.update({prop['name']: prop[non_null_field]})

            features.append(feature)
            feature_collection = FeatureCollection(features)
            named_locations.append(feature_collection)
    return named_locations


def get_by_asset(connection, asset_id):
    """
    Find named locations by asset ID.
    :param connection: A database connection.
    :param asset_id: The asset ID.
    :return:
    """
    sql = '''
                select
                    is_asset_location.nam_locn_id,
                    nam_locn.nam_locn_name,
                    type.type_name
                from
                    is_asset_location
                join
                    is_asset on is_asset.asset_uid = is_asset_location.asset_uid
                        and is_asset.asset_uid = :asset_id
                join
                    nam_locn on is_asset_location.nam_locn_id = nam_locn.nam_locn_id
                join
                    type on nam_locn.type_id = type.type_id
            '''
    with closing(connection.cursor()) as cursor:
        res = cursor.execute(sql, asset_id=asset_id)
        row = res.fetchone()
        if row is not None:
            named_location_id = row[0]
            named_location_name = row[1]
            type_name = row[2]
            return {'id': named_location_id, 'name': named_location_name, 'type': type_name}
        return None


def get_asset_history(connection, asset_id):
    """
    Get an asset's location history.
    :param connection: A database connection.
    :param asset_id: The asset ID.
    :return: Geojson FeatureCollection of history.
    """
    sql = '''
            select
                is_asset_location.nam_locn_id,
                is_asset_location.install_date,
                is_asset_location.remove_date,
                is_asset_location.tran_date,
                nam_locn.nam_locn_name,
                type.type_name
            from
                is_asset_location, nam_locn, type
            where
                is_asset_location.asset_uid = :asset_id
            and
                nam_locn.nam_locn_id = is_asset_location.nam_locn_id
            and
                type.type_id = nam_locn.type_id
        '''
    with closing(connection.cursor()) as cursor:
        cursor.prepare(sql)
        rows = cursor.execute(None, asset_id=asset_id)
        named_locations = []
        for row in rows:
            named_location_id = row[0]
            install_date = date_formatter.format(row[1])
            remove_date = date_formatter.format(row[2])
            tran_date = date_formatter.format(row[3])
            named_location_name = row[4]

            locations = location_finder.get_all(connection, named_location_id)
            properties = get_properties(connection, named_location_id)
            parents = get_parents(connection, named_location_id)
            context = get_context(connection, named_location_id)

            # The parent value only contains site
            site = parents[0]['name']

            named_location = Feature(properties={'name': named_location_name,
                                                 'site': site,
                                                 'install_date': install_date,
                                                 'remove_date': remove_date,
                                                 'transaction_date': tran_date,
                                                 'context': context,
                                                 'locations': locations})
            for prop in properties:
                non_null_field = 'string_value'
                if prop['number_value'] is not None:
                    non_null_field = 'number_value'
                if prop['date_value'] is not None:
                    non_null_field = 'date_value'
                named_location.update({prop['name']: prop[non_null_field]})

            named_locations.append(named_location)

    return FeatureCollection(named_locations)


def add_reference_locations(connection, named_locations):
    """
    Add the named location reference locations to the Dictionary.
    :param connection: A database connection.
    :param named_locations: Dictionary containing named locations.
    :return:
    """
    for named_location in named_locations:
        locations = named_location.get('locations')
        for location in locations:
            coordinates = location.get('coordinates')
            reference_id = coordinates.get('reference_id')
            locations = location_finder.get_all(connection, reference_id)
            location.update({'reference_locations': locations})


def get_properties(connection, named_location_id):
    """
    Get a list of properties associated with a named location.
    :param connection: A database connection.
    :param named_location_id: The named location ID to search.
    :return: Dictionary of properties.
    """
    sql = '''
            select
                attr.attr_name,
                attr.attr_desc,
                property.string_value,
                property.number_value,
                property.date_value
            from
                property
            join
                attr on property.attr_id = attr.attr_id
            where
                property.nam_locn_id = :named_location_id
        '''
    with closing(connection.cursor()) as cursor:
        rows = cursor.execute(sql, named_location_id=named_location_id)
        properties = []
        for row in rows:
            name = row[0]
            description = row[1]
            string_value = row[2]
            number_value = row[3]
            date_value = date_formatter.format(row[4])
            prop = {
                'name': name,
                'description': description,
                'string_value': string_value,
                'number_value': number_value,
                'date_value': date_value
            }
            properties.append(prop)
        return properties


def get_context(connection, named_location_id):
    """
    Get context entries for a named location.
    :param connection: A database connection.
    :param named_location_id: The named location ID.
    :return: List of context codes.
    """
    sql = 'select context_code from nam_locn_context where nam_locn_id = :named_location_id'
    with closing(connection.cursor()) as cursor:
        rows = cursor.execute(sql, named_location_id=named_location_id)
        context_codes = []
        for row in rows:
            context_code = row[0]
            context_codes.append(context_code)
        return context_codes


def get_parents(connection, named_location_id):
    """
    Get the parents of a named location.
    :param connection: A database connection.
    :param named_location_id:
    :return: List of Dictionaries containing parent data.
    """
    sql = '''
            select
                prnt_nam_locn_id, nam_locn.nam_locn_name, type.type_name
            from
                nam_locn_tree
            join
                nam_locn on nam_locn.nam_locn_id = nam_locn_tree.prnt_nam_locn_id
            join
                type on type.type_id = nam_locn.type_id
            where
                chld_nam_locn_id = :named_location_id
        '''
    parents = []
    with closing(connection.cursor()) as cursor:
        cursor.prepare(sql)
        add_parent(cursor, named_location_id, parents)
        return parents


def add_parent(cursor, named_location_id, parents):
    """
    Recursively get named location parent IDs, names, and types.
    :param cursor: A database cursor object.
    :param named_location_id: The location ID.
    :param parents: Dictionary of parents to append to.
    :return:
    """
    res = cursor.execute(None, named_location_id=named_location_id)
    row = res.fetchone()
    if row is not None:
        parent_id = row[0]
        parent_name = row[1]
        type_name = row[2]
        # Only include the site.
        if type_name.lower() == 'site':
            parents.append({'id': parent_id, 'name': parent_name, 'type': type_name})
        add_parent(cursor, parent_id, parents)
