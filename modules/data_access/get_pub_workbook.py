from contextlib import closing
from typing import NamedTuple, List, Dict
import structlog

from psycopg2.extras import DictCursor

from data_access.db_connector import DbConnector
from data_access.types.pub_workbook import PubWorkbookRow
from data_access.types.pub_workbook import PubWorkbook

log = structlog.get_logger()


def get_pub_workbook(connector: DbConnector, data_product_id: str) -> PubWorkbook:
    """Read a publication workbook from the database for the given data product identifier."""
    workbook_rows: List[PubWorkbookRow] = []
    schema = connector.get_schema()
    connection = connector.get_connection()
    log.debug(f'Made database connection: {connection}')
    sql = f'''
        select
            pfd.rank as rank,
            dpc.dp_name,
            ptd.dp_idq,
            ptd.dp_idq_source,
            pfd.dp_number,
            ptd.name as table_name,
            ptd.description as table_description,
            pfd.field_name,
            pfd.description as field_description,
            pfd.data_type_code,
            pfd.unit_name,
            pfd.meas_scale,
            pfd.ontology_mapping,
            pfd.pub_format,
            pfd.example_entry,
            ptd.usage,
            pfd.field_type,
            ptd.table_type,
            pfd.inputs,
            ptd.filter_sample_class,
            pfd.time_index,
            pfd.time_description,
            pfd.download_package,
            pfd.data_category,
            pfd.sample_info,
            pfd.lov_code,
            pfd.primary_key,
            pfd.redaction_flag
        from 
            {schema}.pub_field_def pfd, {schema}.pub_table_def ptd, {schema}.dp_catalog dpc
        where 
            pfd.pub_table_def_id = ptd.pub_table_def_id 
        and 
            ptd.dp_idq = %s
        and
            dpc.dp_idq = ptd.dp_idq
        order by
            table_name, rank
    '''
    with closing(connection.cursor(cursor_factory=DictCursor)) as cursor:
        cursor.execute(sql, [data_product_id])
        rows = cursor.fetchall()
        log.debug(f'Retrieved {len(rows)} rows of the publication workbook for {data_product_id}')
        for row in rows:
            workbook_row = PubWorkbookRow(rank = row['rank'],
                                          DPName = row['dp_name'],
                                          dpID = row['dp_idq'],
                                          dpIDSource = row['dp_idq_source'],
                                          DPNumber = row['dp_number'],
                                          table = row['table_name'],
                                          tableDescription = row['table_description'],
                                          fieldName = row['field_name'],
                                          description = row['field_description'],
                                          dataType = row['data_type_code'],
                                          units = row['unit_name'],
                                          measurementScale = row['meas_scale'],
                                          ontologyMapping = row['ontology_mapping'],
                                          pubFormat = row['pub_format'],
                                          exampleEntry = row['example_entry'],
                                          usage = row['usage'],
                                          fieldType = row['field_type'],
                                          tableType = row['table_type'],
                                          inputs = row['inputs'],
                                          filterSampleClass = row['filter_sample_class'],
                                          timeIndex = row['time_index'],
                                          timeDescription = row['time_description'],
                                          downloadPkg = row['download_package'],
                                          dataCategory = row['data_category'],
                                          sampleInfo = row['sample_info'],
                                          lovName = row['lov_code'],
                                          primaryKey = row['primary_key'],
                                          redactionFlag = row['redaction_flag'])
            workbook_rows.append(workbook_row)
    return PubWorkbook(workbook_rows)
