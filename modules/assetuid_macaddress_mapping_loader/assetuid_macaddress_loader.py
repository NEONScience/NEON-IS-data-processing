#!usr/bin/env python3
import os
from pathlib import Path
from structlog import get_logger
import requests
import common.log_config as log_config


log = get_logger()


def load() -> None:
    out_path: str = os.environ['OUTPUT_PATH']
    region: str = os.environ['REGION']
    log_level: str = os.environ['LOG_LEVEL']
    log_config.configure(log_level)
    source_type: str = os.environ['SOURCE_TYPE']
    log.debug(f'out_path: {out_path}')
    

    mount_path= Path('/var/cds_secret')
    cds_hostname_file = Path(mount_path, "hostname")
    cds_url_path = cds_hostname_file.read_text()
    url_path = f"{cds_url_path}/assets?sensor-type-name={source_type}"

    log.debug(f"url_path is {url_path}")
    response = requests.get(url_path, headers={'Accept': 'application/json'})
    if response.status_code == 200:
        log.debug("successfully fetched the data")
        assets = response.json()['assets']
        log.debug('asset details are:')
        for asset in assets:
            try:
                asset_uid = asset.get("assetUid")
                mac_address = asset.get("macAddress")
                if mac_address is None:
                    log.debug(f'Empty mac_address for asset_uid {asset.get("assetUid")}')
                    continue

                file_name = f'{source_type}_{mac_address}.txt'
                file_path = Path(out_path, source_type, mac_address, file_name)
                # print(f"filePath to write is: {file_path}")
                file_path.parent.mkdir(parents=True, exist_ok=True)
                with open(file_path, 'w') as asset_macaddress_file:
                    asset_macaddress_file.write(str(asset_uid))
            except BaseException as err:
                log.error(f"Unexpected {asset_uid}, {mac_address}")
                raise err
    else:
        log.error(f"Asset call failed with error code: {response.status_code}.")


if __name__ == '__main__':
    load()
