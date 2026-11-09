#!/usr/bin/env python3
from contextlib import closing
from typing import Iterable, List, Optional

from data_access.db_connector import DbConnector
from data_access.types.cfgloc_ver import CfglocVer


def get_enviroscan_cfgloc_vers(
    connector: DbConnector,
    sites: Optional[Iterable[str]] = None,
) -> List[CfglocVer]:
    """
    Return the VER positions actually configured for each CFGLOC in the
    `conc-h2o-soil-salinity-split_*` group prefix.

    Corresponds to Q4 in swc_loc_depths_notes.md. This governs which VERs the
    loader emits: a CFGLOC missing a lower depth simply won't have that VER in
    the group table, so it won't be emitted. Consistent with what `group_split`
    produces upstream.

    :param connector: A database connection.
    :param sites:     Optional list of 4-char NEON site codes. When set, only
                      returns rows whose group_name matches one of the given
                      sites (parsed from the split group's site-code suffix).
    :return: One entry per (CFGLOC, HOR, VER) that appears in the split group.
    """
    connection = connector.get_connection()
    schema = connector.get_schema()
    site_list = list(sites) if sites else None
    if site_list:
        site_filter_where = (
            "and substring(g.group_name from "
            "'conc-h2o-soil-salinity-split_(....)') = ANY(%s)"
        )
    else:
        site_filter_where = ''
    sql = f'''
        select
            nl.nam_locn_name,
            g.hor,
            g.ver,
            g.group_name
        from {schema}.named_location_group nlg
        join {schema}."group"    g  on nlg.group_id = g.group_id
        join {schema}.nam_locn   nl on nlg.named_location_id = nl.nam_locn_id
        where g.group_name like 'conc-h2o-soil-salinity-split\\_%%' escape '\\'
          {site_filter_where}
        order by nl.nam_locn_name, g.hor, g.ver
    '''
    entries: List[CfglocVer] = []
    with closing(connection.cursor()) as cursor:
        if site_list:
            cursor.execute(sql, (site_list,))
        else:
            cursor.execute(sql)
        for row in cursor.fetchall():
            entries.append(CfglocVer(
                cfgloc=row[0],
                hor=row[1],
                ver=row[2],
                group_name=row[3],
            ))
    return entries
