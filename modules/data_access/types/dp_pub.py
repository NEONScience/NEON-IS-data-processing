from typing import NamedTuple,List,Optional
from datetime import datetime


class DpPub(NamedTuple):
    dataProductId: str
    site: str
    dataIntervalStart: Optional[datetime]
    dataIntervalEnd: Optional[datetime]
    packageType: str
    hasData: str
    status: str
    create_date: Optional[datetime]
    updateDate: Optional[datetime]
    releaseStatus: str
    id: int

