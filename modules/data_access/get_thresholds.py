from contextlib import closing
from typing import List, Iterator

import common.date_formatter as date_formatter
from data_access.types.threshold import Threshold
from data_access.get_threshold_context import get_threshold_context
from data_access.db_connector import DbConnector


def get_thresholds(connector: DbConnector, term: str) -> Iterator[Threshold]:
    connection = connector.get_connection()
    schema = connector.get_schema()
    sql = f'''
         select
             attr.column_name,
             threshold.term_name,
             threshold.threshold_uuid,
             nam_locn.nam_locn_name,
             condition.start_date,
             condition.end_date,
             condition.is_date_constrained,
             condition.start_day_of_year,
             condition.end_day_of_year,
             property.number_value,
             property.string_value
         from
             {schema}.condition
         join
             {schema}.threshold on condition.threshold_uuid = threshold.threshold_uuid
         join
             {schema}.property on condition.condition_uuid = property.condition_uuid
         join
             {schema}.attr on attr.attr_id = property.attr_id
         join
             {schema}.nam_locn on nam_locn.nam_locn_id = condition.nam_locn_id
         where
             property.condition_uuid is not null
         and 
             threshold.term_name = ANY (%s)
         order by
             nam_locn.nam_locn_name, threshold.term_name, threshold.threshold_name
     '''
    with closing(connection.cursor()) as cursor:
        if term == 'none':
            cursor.execute(sql.replace("and \n             threshold.term_name = ANY (%s)", ""))
        else:
            terms = term.split("|")
            cursor.execute(sql, (terms,))
        rows = cursor.fetchall()
        for row in rows:
            threshold_name = row[0]
            term_name = row[1]
            threshold_uuid = row[2]
            location_name = row[3]
            start_date = row[4]
            end_date = row[5]
            is_date_constrained = row[6]
            start_day_of_year = row[7]
            end_day_of_year = row[8]
            number_value = row[9]
            string_value = row[10]
            '''
            Change term name for soil temp thresholds from 'soilPRTResistance' to 'temp'. 
            The Avro schema for calibrated PRT data uses the term name 'temp' 
            and data file term name must match threshold term name to apply QA/QC.
            '''
            if term_name == 'soilPRTResistance':
                term_name = 'temp'
            if start_date is not None:
                start_date = date_formatter.to_string(start_date)
            if end_date is not None:
                end_date = date_formatter.to_string(end_date)
            if number_value is not None:
                number_value = float(number_value)
            context: List[str] = get_threshold_context(connector, threshold_uuid)
            threshold = Threshold(threshold_name=threshold_name,
                                  term_name=term_name,
                                  location_name=location_name,
                                  context=context,
                                  start_date=start_date,
                                  end_date=end_date,
                                  is_date_constrained=is_date_constrained,
                                  start_day_of_year=start_day_of_year,
                                  end_day_of_year=end_day_of_year,
                                  number_value=number_value,
                                  string_value=string_value)
            yield threshold
