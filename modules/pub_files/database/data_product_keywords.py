from contextlib import closing
from typing import List

from data_access.db_connector import DbConnector


def get_keywords(connector: DbConnector, data_product_id: str) -> List[str]:
    """Returns the keywords associated with a data product."""
    connection = connector.get_connection()
    schema = connector.get_schema()
    sql = f'''
        select 
            keyword.word 
        from 
            {schema}.keyword, {schema}.dp_keyword
        where
            dp_keyword.dp_idq = %s
        and 
            dp_keyword.keyword_id = keyword.keyword_id
    '''
    keywords = []
    with closing(connection.cursor()) as cursor:
        cursor.execute(sql, [data_product_id])
        rows = cursor.fetchall()
        for row in rows:
            keyword = row[0]
            keywords.append(keyword)
    return keywords
