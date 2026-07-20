#!/usr/bin/env python3
from contextlib import closing
from typing import Iterable, List, Optional

from data_access.db_connector import DbConnector
from data_access.types.cvald1_calibration import Cvald1Calibration


def get_enviroscan_cvald1_calibrations(
    connector: DbConnector,
    sites: Optional[Iterable[str]] = None,
) -> List[Cvald1Calibration]:
    """
    Return every CVALD1 calibration for enviroscan probes across all assets and time,
    filtered to the VSWC streams only (rawVSWC0..rawVSWC7 → VER 501..508).

    Corresponds to Q2 in swc_loc_depths_notes.md. Even `sensor_stream_num` values
    (0,2,4,6,8,10,12,14) are VSWC; odd are VSIC. VSIC shares the same physical
    depth so is excluded here — depth math uses only VSWC.

    :param connector: A database connection.
    :param sites:     Optional list of 4-char NEON site codes. When set, restricts
                      the result to calibrations for assets that have EVER been
                      installed at a CFGLOC in the `conc-h2o-soil-salinity-split_<SITE>*`
                      groups. Cross-site cals are still included per-asset so the
                      per-install selector in the loader can pick the right one.
    :return: One entry per (asset × valid-period × VSWC stream).
    """
    connection = connector.get_connection()
    schema = connector.get_schema()
    site_list = list(sites) if sites else None
    if site_list:
        site_filter_where = f'''
          and cal.asset_uid in (
              select ial.asset_uid
              from {schema}.is_asset_location    ial
              join {schema}.named_location_group nlg on nlg.named_location_id = ial.nam_locn_id
              join {schema}."group"              g   on nlg.group_id = g.group_id
              where substring(g.group_name from
                              'conc-h2o-soil-salinity-split_(....)') = ANY(%s)
          )
        '''
    else:
        site_filter_where = ''
    # DISTINCT: is_ingest_term can carry multiple historical rows per
    # (asset_definition_uuid, stream_id) when the asset_definition has been
    # versioned. Assets on a repeatedly-updated definition would otherwise
    # fan out N-fold. See CFGLOC112096 asset 33113 (3 rows) vs assets 17834/7524
    # (1 row each) for a real-world example.
    sql = f'''
        select distinct
            cal.asset_uid,
            cal.calibration_id,
            cal.sensor_stream_num,
            it.schema_field_name,
            cal.valid_start_time,
            cal.valid_end_time,
            cal.cert_filename,
            mta.value
        from {schema}.calibration          cal
        join {schema}.calibration_metadata mta  on cal.calibration_id = mta.calibration_id
        join {schema}.is_asset_assignment  asgn on asgn.asset_uid = cal.asset_uid
        join {schema}.is_asset_definition  def  on def.asset_definition_uuid = asgn.asset_definition_uuid
        join {schema}.is_sensor_type       st   on st.sensor_type_name = def.sensor_type_name
        join {schema}.is_ingest_term       it   on it.asset_definition_uuid = def.asset_definition_uuid
                                              and it.stream_id = cal.sensor_stream_num
        where mta.name = 'CVALD1'
          and st.avro_schema_name = 'enviroscan'
          and it.schema_field_name like 'rawVSWC%%'
          {site_filter_where}
        order by cal.asset_uid, cal.valid_start_time, it.schema_field_name
    '''
    calibrations: List[Cvald1Calibration] = []
    with closing(connection.cursor()) as cursor:
        if site_list:
            cursor.execute(sql, (site_list,))
        else:
            cursor.execute(sql)
        for row in cursor.fetchall():
            calibrations.append(Cvald1Calibration(
                asset_uid=row[0],
                calibration_id=row[1],
                sensor_stream_num=row[2],
                schema_field_name=row[3],
                valid_start_time=row[4],
                valid_end_time=row[5],
                cert_filename=row[6],
                cvald1_cm=float(row[7]),
            ))
    return calibrations
