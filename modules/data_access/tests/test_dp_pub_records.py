#!/usr/bin/env python3
import os
from pathlib import Path
from typing import List,Iterator

import unittest

from datetime import datetime
from data_access.types.dp_pub import DpPub


class DpPubRecordsTest(unittest.TestCase):

    def test_dp_pub_records(self):
        dp_id = 'dp_id'
        data_begin = '2013-10-01T00:00:00Z'
        data_cutoff = '2013-11-01T00:00:00Z'
        site = 'SITE'

        dataProductId = 'NEON.DOM.SITE.DP1.00017.001'
        dataIntervalStart = '2013-10-01T00:00:00Z'
        dataIntervalEnd = '2013-11-01T00:00:00Z'
        packageType = 'p'
        hasData = 'h'
        status = 's'
        create_date = '2013-10-01T00:00:00Z'
        updateDate = '2013-11-01T00:00:00Z'
        releaseStatus = 'r'
        id = 12345

        def get_dp_pubs(dp_id: str,data_begin: datetime,data_cutoff: datetime,site: str) -> Iterator[DpPub]:
            """
            Mock function for getting dp pub records.

            :return: A dp pub record.
            """

            dppub = DpPub(dataProductId=dataProductId,
                          site=site,
                          dataIntervalStart=dataIntervalStart,
                          dataIntervalEnd=dataIntervalEnd,
                          packageType=packageType,
                          hasData=hasData,
                          status=status,
                          create_date=create_date,
                          updateDate=updateDate,
                          releaseStatus=releaseStatus,
                          id=id)
            yield dppub

        # test the function
        for dppub in get_dp_pubs(dp_id,data_begin,data_cutoff,site):
            self.assertTrue(dppub[0] == dataProductId)
            self.assertTrue(dppub[1] == site)
            self.assertTrue(dppub[2] == dataIntervalStart)
            self.assertTrue(dppub[3] == dataIntervalEnd)
            self.assertTrue(dppub[4] == packageType)
            self.assertTrue(dppub[5] == hasData)
            self.assertTrue(dppub[6] == status)
            self.assertTrue(dppub[7] == create_date)
            self.assertTrue(dppub[8] == updateDate)
            self.assertTrue(dppub[9] == releaseStatus)
            self.assertTrue(dppub[10] == id)
