#!/usr/bin/env python3

from data_access.types.dp_pub import DpPub


def get_dp_pub_data():
     return {
                DpPub(dataProductId = 'NEON.DOM.SITE.DP1.00040.001', site = 'MD01',
                       dataIntervalStart = '2023-03-01 00:00:00.000',
                       dataIntervalEnd = '2023-04-01 00:00:00.000',
                       packageType = 'basic',hasData = 'N', status = 'NODATA',
                       create_date = '2023-04-03 22:52:05.103',
                       updateDate = '2023-04-03 22:53:03.116',
                       releaseStatus = 'p', id = 14478336),
                DpPub(dataProductId = 'NEON.DOM.SITE.DP1.00040.001', site = 'MD01',
                      dataIntervalStart = '2023-03-01 00:00:00.000',
                      dataIntervalEnd = '2023-04-01 00:00:00.000',
                      packageType = 'expanded',hasData = 'N', status = 'NODATA',
                      create_date = '2023-04-03 22:52:05.103',
                      updateDate = '2023-04-03 22:53:03.116',
                      releaseStatus = 'p', id = 14478337),
                DpPub(dataProductId = 'NEON.DOM.SITE.DP1.00040.001', site = 'YELL',
                        dataIntervalStart = '2023-03-01 00:00:00.000',
                        dataIntervalEnd = '2023-04-01 00:00:00.000',
                        packageType = 'basic', hasData = 'Y', status = 'OK',
                        create_date = '2023-04-03 22:52:05.103',
                        updateDate = '2023-04-03 22:53:03.116',
                        releaseStatus = 'p', id = 14478392),
                DpPub(dataProductId = 'NEON.DOM.SITE.DP1.00040.001', site = 'YELL',
                      dataIntervalStart = '2023-03-01 00:00:00.000',
                        dataIntervalEnd = '2023-04-01 00:00:00.000',
                        packageType = 'expanded', hasData = 'Y', status = 'OK',
                        create_date = '2023-04-03 22:52:05.103',
                        updateDate = '2023-04-03 23:17:50.032',
                        releaseStatus = 'p', id = 14478393)
             }

