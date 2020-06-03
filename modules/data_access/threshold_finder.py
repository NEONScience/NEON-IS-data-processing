#!/usr/bin/env python3
from contextlib import closing

import lib.date_formatter as date_formatter


def find_thresholds(connection):
    """
    Return all thresholds.

    :param connection: A database connection.
    :type connection: connection object
    :return: Threshold data.
    """
    query = '''
            select
                attr.column_name,
                threshold.term_name,
                nam_locn.nam_locn_name,
                condition.condition_uuid,
                condition.start_date,
                condition.end_date,
                condition.is_date_constrained,
                condition.start_day_of_year,
                condition.end_day_of_year,
                property.number_value,
                property.string_value
            from
                pdr.condition
            join
                pdr.threshold on condition.threshold_uuid = threshold.threshold_uuid
            join
                pdr.property on condition.condition_uuid = property.condition_uuid
            join
                pdr.attr on attr.attr_id = property.attr_id
            join
                pdr.nam_locn on pdr.nam_locn.nam_locn_id = pdr.condition.nam_locn_id
            where
                property.condition_uuid is not null
            order by
                nam_locn.nam_locn_name
        '''
    thresholds = []
    with closing(connection.cursor()) as cursor:
        rows = cursor.execute(query)
        for row in rows:
            threshold = {}
            threshold.update({'threshold_name': row[0]})
            '''
            Change term name for soil temp thresholds from 'soilPRTResistance' to 'temp'. 
            The Avro schema for calibrated prt data changes the term name to 'temp' 
            and the term name in the data file must match the term name in the thresholds
            to apply QA/QC.
            '''
            term_name = row[1]
            if term_name == 'soilPRTResistance':
                term_name = 'temp'
            threshold.update({'term_name': term_name})
            threshold.update({'location_name': row[2]})
            threshold.update({'context': find_context(connection, row[3])})
            threshold.update({'start_date': date_formatter.convert(row[4])})
            threshold.update({'end_date': date_formatter.convert(row[5])})
            threshold.update({'is_date_constrained': row[6]})
            threshold.update({'start_day_of_year': row[7]})
            threshold.update({'end_day_of_year': row[8]})
            threshold.update({'number_value': row[9]})
            threshold.update({'string_value': row[10]})
            thresholds.append(threshold)
    return thresholds


def find_context(connection, condition_uuid: str):
    """
    Find all context entries for a threshold.

    :param connection: Database connection.
    :type connection: connection object
    :param condition_uuid: The condition UUID.
    :return: The context codes.
    """
    with closing(connection.cursor()) as cursor:
        query = 'select context_code from condition_context where condition_uuid = :condition_uuid'
        rows = cursor.execute(query, condition_uuid=condition_uuid)
        context_codes = []
        for row in rows:
            context_codes.append(row[0])
        return context_codes
