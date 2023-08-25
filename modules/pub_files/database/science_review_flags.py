from contextlib import closing
from datetime import datetime
from typing import NamedTuple, List

from psycopg2.extras import DictCursor

from data_access.db_connector import DbConnector


class ScienceReviewFlag(NamedTuple):
    id: int
    start_date: datetime
    end_date: datetime
    stream_name: str
    user_name: str
    user_comment: str
    flag: int
    create_date: datetime
    last_update: datetime


def make_get_flags(connector: DbConnector):
    """
    Returns a function accepting a data product ID and a site code and returning a list
    of science review flags.
    """

    def get_flags(data_product_id: str, site: str, start_date: datetime, end_date: datetime) -> List[ScienceReviewFlag]:
        flags = []
        connection = connector.get_connection()
        schema = connector.get_schema()
        sql = f'''
            select 
                id,
                start_date,
                end_date,
                meas_strm_name,
                username,
                user_comment,
                srf,
                create_date,
                last_update    
            from 
                {schema}.science_review 
            where 
                meas_strm_name like '%%{data_product_id}%%'
            and 
                meas_strm_name like '%%{site}%%'
            and
                start_date <= %(data_end_date)s and end_date >= %(data_start_date)s
            order by 
                id desc
        '''
        with closing(connection.cursor(cursor_factory=DictCursor)) as cursor:
            cursor.execute(sql, dict(data_start_date=start_date, data_end_date=end_date))
            rows = cursor.fetchall()
            for row in rows:
                flag_id = row['id']
                start_date = row['start_date']
                end_date = row['end_date']
                stream_name = row['meas_strm_name']
                user_name = row['username']
                user_comment = row['user_comment']
                flag = row['srf']
                create_date = row['create_date']
                last_update = row['last_update']
                flags.append(ScienceReviewFlag(id=flag_id,
                                               start_date=start_date,
                                               end_date=end_date,
                                               stream_name=stream_name,
                                               user_name=user_name,
                                               user_comment=user_comment,
                                               flag=flag,
                                               create_date=create_date,
                                               last_update=last_update))
        return flags

    return get_flags
