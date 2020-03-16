from contextlib import closing

from geojson import Feature, FeatureCollection
import structlog

import data_access.location_finder as location_finder
import lib.date_formatter as date_formatter

log = structlog.get_logger()


def get_active_periods(connection, named_location_id, cutoff_date=None):
    """
    Get the active time periods for a named location.
    :param connection: A database connection
    :param named_location_id: The ID to search on.
    :param cutoff_date: The end time for periods with no set end time.
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
            start_date = row[0]
            if start_date is not None:
                end_date = row[1]
                if cutoff_date is not None:
                    if end_date is None:
                        end_date = cutoff_date
                    elif end_date > cutoff_date:
                        end_date = cutoff_date
                periods.append({'start_date': date_formatter.convert(start_date),
                                'end_date': date_formatter.convert(end_date)})
        return periods


def get_by_type(connection, type_name, cutoff_date=None):
    """
    Get named locations in GEOJson format by type and cutoff date for the active time range.
    :param connection: A database connection.
    :param type_name: The named location type.
    :param cutoff_date: The maximum active end time to return.
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
            /*
            and 
                nam_locn_context.context_code = :context
            */
            /* fetch first 1 rows only */
        '''
    with closing(connection.cursor()) as cursor:
        cursor.prepare(sql)
        rows = cursor.execute(None, type_name=type_name)
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
            active_periods = get_active_periods(connection, named_location_id, cutoff_date=cutoff_date)

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
    :return: Dictionary with ID, name, and type of asset's location.
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
            install_date = date_formatter.convert(row[1])
            remove_date = date_formatter.convert(row[2])
            tran_date = date_formatter.convert(row[3])
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
            date_value = date_formatter.convert(row[4])
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
    :return: List of context entries.
    """
    sql = '''
        select
            context_code,
            context_group_id
        from 
            nam_locn_context 
        where 
            nam_locn_id = :named_location_id
        '''
    with closing(connection.cursor()) as cursor:
        rows = cursor.execute(sql, named_location_id=named_location_id)
        contexts = []
        for row in rows:
            context_code = row[0]
            group = row[1]
            if group is None:
                contexts.append(context_code)
            else:
                group = context_code + '-' + str(group)
                contexts.append(group)
        return contexts


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
    """
    res = cursor.execute(None, named_location_id=named_location_id)
    row = res.fetchone()
    if row is not None:
        parent_id = row[0]
        parent_name = row[1]
        type_name = row[2]
        if type_name.lower() == 'site':  # Only include the site.
            parents.append({'id': parent_id, 'name': parent_name, 'type': type_name})
        add_parent(cursor, parent_id, parents)


def get_site(connection, named_location_id):
    parents = get_parents(connection, named_location_id)
    for parent in parents:
        if parent.get('type').lower() == 'site':
            site_name = parent.get('name')
            return site_name
    return None


def get_schema_name(connection, named_location_name):
    """
    Return the schema name for the type of sensor the named location accepts.
    :param connection: A database connection.
    :param named_location_name: The named location name.
    :return:
    """
    sql = '''
        select distinct
            avro_schema_name
        from 
            is_sensor_type, is_asset_definition, is_asset_assignment, is_asset_location, nam_locn
        where
            is_sensor_type.sensor_type_name = is_asset_definition.sensor_type_name
        and 
            is_asset_definition.asset_definition_uuid = is_asset_assignment.asset_definition_uuid
        and 
            is_asset_assignment.asset_uid = is_asset_location.asset_uid
        and 
            is_asset_location.nam_locn_id = nam_locn.nam_locn_id
        and 
            nam_locn.nam_locn_name = :name
    '''
    with closing(connection.cursor()) as cursor:
        cursor.prepare(sql)
        cursor.execute(None, name=named_location_name)
        row = cursor.fetchone()
        if row is not None:
            name = row[0]
            if name is not None:
                return name
        return None
