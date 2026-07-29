#!/usr/bin/env python3
from datetime import datetime
from typing import NamedTuple, Optional


class Cvald1Calibration(NamedTuple):
    asset_uid: int
    calibration_id: int
    sensor_stream_num: int
    schema_field_name: str
    valid_start_time: Optional[datetime]
    valid_end_time: Optional[datetime]
    cert_filename: Optional[str]
    cvald1_cm: float
