#!/usr/bin/env python3
from datetime import datetime
from typing import NamedTuple, Optional


class AssetInstall(NamedTuple):
    cfgloc: str
    cfgloc_description: Optional[str]
    nam_locn_id: int
    asset_uid: int
    install_date: Optional[datetime]
    remove_date: Optional[datetime]
