#!/bin/bash

# Create source pipelines and repos.

pachctl start transaction

# Import trigger
pachctl create repo import_trigger

python3 load_import_trigger.py

# PRT data 
pachctl create repo data_source_prt

# Dualfan data
pachctl create repo data_source_dualfan 

# 2D wind data
pachctl create repo data_source_windobserverii

# Thresholds
pachctl create pipeline -f threshold_loader/threshold_loader.json

# Locations
pachctl create pipeline -f location_asset/location_asset.json

# Calibrations
pachctl create repo calibration

# Mock heater events
pachctl create repo heater_event

# Avro schemas
pachctl create repo avro_schemas 

# FDAS uncertainty
pachctl create repo uncertainty_fdas 

pachctl finish transaction
