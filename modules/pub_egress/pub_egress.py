#!/usr/bin/env python3
import os
import re
from pathlib import Path
import datetime
from dateutil.relativedelta import relativedelta
from common.err_datum import err_datum_path
import pandas as pd

from structlog import get_logger
from urllib.parse import urlsplit

log = get_logger()


class Pub_egress:

    def __init__(self, data_path: Path, starting_path_index: int, out_path: Path, out_path_mdp: Path,
                 out_mdp_sites: Path, err_path: Path, egress_url: str, prod: str, staging: str) -> None:
        """
        Constructor.

        :param data_path: The data path.
        :param out_path: The output path for writing results.
        :param err_path: The error directory, i.e., errored.
        """
        self.data_path = data_path
        self.starting_path_index = starting_path_index
        self.out_path = out_path
        self.out_path_mdp = out_path_mdp
        self.out_mdp_sites = out_mdp_sites
        # DirErrBase: the user specified error directory, i.e., /errored
        self.DirErrBase = Path(err_path)
        self.idq_length = 6
        self.domain_index = 1
        self.site_index = 2
        self.date_index = 10
        self.package_index = 11
        self.filename_delimiter = "."
        self.domain_generic = "DOM"
        self.site_generic = "SITE"
        self.date_delimiter = "-"
        self.date_format = '%Y%m%dT%H%M%S'
        self.date_range_delimiter = "--"
        self.delimited_data_length = 14 # length of delimited filename for data files. Used to find data files.
        self.egress_prefix = egress_url + "/"
        self.prod = prod
        self.staging = staging
        self._lookup_mdp_path = None

    def upload(self) -> None:

        data_path_start = Path(*self.data_path.parts[0:self.starting_path_index+1]) # starting index
        for path in data_path_start.rglob('*'):
            # When we reach a manifest file, we have found a pub package to process
            if path.parts[-1] == 'manifest.csv':
                package_path = path.parent
                log.info(f'Processing pub package {package_path}')

                object_id_by_file = {}

                # Reset each package
                idq = None
                site = None
                date_range = None
                package = None

                try:
                    # Open the manifest file
                    manifest = pd.read_csv(path)

                    # Get to a data file to read the idq, site, date range, and package
                    for file_path in package_path.rglob('*'):
                        filename = file_path.parts[-1]
                        filename_parts = filename.split(self.filename_delimiter)
                        if len(filename_parts) == self.delimited_data_length:
                            site = filename_parts[self.site_index]
                            # parse out the idq
                            filename_parts[self.domain_index] = self.domain_generic
                            filename_parts[self.site_index] = self.site_generic
                            idq = self.filename_delimiter.join(filename_parts[:self.idq_length])

                            # construct date range field
                            date = filename_parts[self.date_index]
                            date_parts = date.split(self.date_delimiter)
                            year = int(date_parts[0])
                            month = int(date_parts[1])
                            start_date = datetime.date(year, month, 1)
                            next_month = start_date + relativedelta(days=+32)
                            end_date = datetime.date(next_month.year, next_month.month, 1)
                            date_range = start_date.strftime(
                                self.date_format) + self.date_range_delimiter + end_date.strftime(self.date_format)
                            package = filename_parts[self.package_index]

                            break

                    # Now run through all the files, writing to output
                    for root, dirs, files in os.walk(str(package_path)):
                        for filename in files:
                            # ignore the manifest file
                            if 'manifest.csv' in filename:
                                continue
                            # Get portal visibility. Skip egress if private
                            visibility=manifest.loc[manifest['file'] == filename, 'visibility']
                            log.debug(f'Visibility for {filename}: {visibility.iloc[0]}')
                            if (not (re.match(r'^MD(\d\d)',site))) and (visibility.iloc[0] != 'public'):
                                continue

                            file_path = Path(root, filename)

                            # construct link filename
                            base_path = os.path.join(idq, site, date_range, package, filename)
                            # construct object ID
                            key = self.filename_delimiter.join(filename.split(self.filename_delimiter))

                            if not re.match(r'^MD(\d\d)',site):
                                link_path = Path(self.out_path, base_path)
                                object_id_by_file[key] = self.egress_prefix + base_path
                            else:
                                link_path = Path(self.out_path_mdp, base_path)
                                object_id_by_file[key] = self.get_mdp_file_path(site) + base_path

                            log.debug(f'source_path: {file_path} link_path: {link_path}')
                            link_path.parent.mkdir(parents=True, exist_ok=True)

                            # Place file in output
                            if not link_path.exists():
                                link_path.symlink_to(file_path)

                    # Populate the object id
                    for key in object_id_by_file:
                        manifest.loc[manifest['file'] == key, 'objectId'] = object_id_by_file[key]

                    # Restrict manifest to private files for MDP sites, i.e., MD03, MD11, ...
                    # and public files for non MDP sites, i.e., ABBY, BARR... and write to the output
                    if re.match(r'^MD(\d\d)',site):
                        manifest = manifest.loc[manifest['visibility'] == 'private',]
                        manifest.to_csv(os.path.join(self.out_path_mdp, idq, site, date_range, package, 'manifest.csv'), index=False)

                    else:
                        manifest = manifest.loc[manifest['visibility'] == 'public',]
                        manifest.to_csv(os.path.join(self.out_path, idq, site, date_range, package, 'manifest.csv'), index=False)
                                            
                except Exception as e:
                    err_datum_path(err=str(e),DirDatm=str(path.parent),DirErrBase=self.DirErrBase,
                                   RmvDatmOut=True,DirOutBase=self.out_path)

    def get_mdp_file_path(self, site: str) -> str:
        if not self.out_mdp_sites:
            raise RuntimeError("OUT_MDP_SITES is not set")
        self._ensure_lookup()
        try:
            mdp_path = self._lookup_mdp_path[(site, self.prod, self.staging)]
        except KeyError:
            raise KeyError(f"No entry for site={site!r}, prod={self.prod}, staging={self.staging}")
        parts = urlsplit(self.egress_prefix)
        return f"{parts.scheme}://{parts.netloc}/{mdp_path}/"

    def _ensure_lookup(self):
        if self._lookup_mdp_path is not None:
            return
        rows = []
        with self.out_mdp_sites.open(encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if not line or line.startswith("#"):
                    continue
                mdp_site, is_prod, is_staging, path = line.split()
                rows.append({
                    "site": mdp_site,
                    "prod": is_prod.lower(),
                    "staging": is_staging.lower(),
                    "path": path,
                })
        self._lookup_mdp_path = {(r["site"], r["prod"], r["staging"]): r["path"] for r in rows}

    def refresh(self):
        """Force re-read of the sites file (e.g., if it changed)."""
        self._lookup_mdp_path = None
