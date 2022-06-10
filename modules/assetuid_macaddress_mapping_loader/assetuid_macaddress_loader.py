#!usr/bin/env python3
import os
import logging
from pathlib import Path
from contextlib import closing
from structlog import get_logger
import environs
import requests
import json
import argparse
import common.log_config as log_config


# log = get_logger()


def load() -> None:
    env = environs.Env
    out_path: Path = env.path('OUTPUT_PATH')
    region: str = env.path('REGION')
    log_level: str = env.log_level('LOG_LEVEL', 'DEBUG')
    log_config.configure(log_level)
    log.debug(f'out_path: {out_path}')
    source_type: str = env.str('SOURCE_TYPE')
    urlpath = ''

    if (region == "int"):
        urlpath = f"http://den-intcdsllb-1.ci.neoninternal.org/cdsWebApp/assets?sensor-type-name={sourcetype}"
    elif (region == "cert"):
        urlpath = f"http://den-certcdsllb-1.ci.neoninternal.org/cdsWebApp/assets?sensor-type-name={sourcetype}"
    elif (region == "prod"):
        urlpath = f"http://den-prodcdsllb-1.ci.neoninternal.org/cdsWebApp/assets?sensor-type-name={sourcetype}"
    else:
        urlpath = f"http://den-intcdsllb-1.ci.neoninternal.org/cdsWebApp/assets?sensor-type-name={sourcetype}"
    log.debug(f"urlpath is {urlpath}")
    response = requests.get(urlpath, headers={'Accept': 'application/json'})
    if response.status_code == 200:
        log.debug("sucessfully fetched the data")
        assets = []
        assets = response.json()['assets']
        log.debug('assets details are:')
        for asset in assets:
            try:
                asset_uid = asset.get("assetUid")
                mac_address = asset.get("macAddress")
                if (mac_address is None):
                    log.debug(f'Empty mac_address for assetuid {asset.get("assetUid")}')
                    continue

                file_name = f'{sourcetype}_{mac_address}.txt'
                file_path = Path(outputpath, sourcetype, mac_address, file_name)
                # print(f"filePath to write is: {file_path}")
                file_path.parent.mkdir(parents=True, exist_ok=True)
                with open(file_path, 'w') as asset_macaddress_file:
                    asset_macaddress_file.write(str(asset_uid))
            except BaseException as err:
                print(f"Unexpected {asset_uid}, {mac_address}")
                raise

    else:
        print(f"Asset call failed with error code: {response.status_code} error with your request")


if __name__ == '__main__':
    # parser = argparse.ArgumentParser()
    # parser.add_argument('--output_path')
    # parser.add_argument('--region')
    # parser.add_argument('--source_type')
    # args = parser.parse_args()
    # outputpath = args.output_path
    # region = args.region
    # sourcetype = args.source_type

    load()
