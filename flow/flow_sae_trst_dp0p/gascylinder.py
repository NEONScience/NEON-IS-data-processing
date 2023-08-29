#!/usr/bin/env python3
from pathlib import Path
from structlog import get_logger
from typing import Dict

import json
import os
import pandas as pd
import xml.etree.ElementTree as ET

from flow_sae_trst_dp0p.l0tol0p import L0toL0p

log = get_logger()


class GasCylinder(L0toL0p):
    """ read calibration files in json, output a metadata json file and a l0p parquet file """

    def __init__(self, cal_term_map, uct_term_map):
        super(GasCylinder, self).__init__(cal_term_map=cal_term_map)
        self.uct_term_map = uct_term_map
        self.day = ''

    def l0tol0p(self) -> None:
        """
        L0 to l0p transformation.
        """
        out_df = pd.DataFrame()
        out_file_path = ''
        # a dictionary to store term:calibration pair
        coeff_dict = {}
        # a dictionary to store uncertainties(attributes/metadata) for all terms
        # as { attr1 : {term1:value11,term2:value21,...}, attr2 : {term1:value12,term2:value22,...} }
        ucrt_dict = {}
        for root, directories, files in os.walk(str(self.in_path)):
            if root.endswith('location'):
                # if self.is_filter_by_context:
                #     self.get_dp_name(root, files)
                out_file_path = self.create_output_path(os.path.dirname(root))
                if self.location_link_type:
                    self.link_location(root, files)
                continue
            if out_file_path and directories and any(tmp_dir in directories for tmp_dir in self.file_dirs):
                self.output_files(out_file_path, coeff_dict, ucrt_dict, out_df)
                coeff_dict = {}
                ucrt_dict = {}
                out_file_path = ''
            if files:
                for file in files:
                    # read calibration xml file
                    calval = ET.parse(root + "/" + file).getroot().find("StreamCalVal")
                    # get calibration coefficients, which are the same in terms' calibration files
                    if not coeff_dict:
                        self.get_cal_coef(calval, coeff_dict)
                    # get uncertainties
                    self.get_ucrt(calval, os.path.basename(root), ucrt_dict)
        self.output_files(out_file_path, coeff_dict, ucrt_dict, out_df)

    def output_files(self, out_file_path: Path, coeff_dict: Dict, ucrt_dict: Dict, out_df: pd.DataFrame) -> None:
        file_part = self.get_data_filename(out_file_path)

        # output calibrations to data/parquet-file
        data_path = Path(out_file_path, 'data', file_part + '.parquet')
        data_path.parent.mkdir(parents=True, exist_ok=True)
        out_df['readout_time'] = pd.date_range(self.day, periods=86400, freq="S")
        for coeff in list(coeff_dict.keys()):
            out_df[coeff] = coeff_dict[coeff]
        self.write_to_parquet(data_path, out_df)
        log.debug(f'coefficients were written to {data_path}.')

        # output uncertainties to metadata/json-file
        metadata_path = Path(out_file_path, 'metadata', file_part + '.json')
        metadata_path.parent.mkdir(parents=True, exist_ok=True)
        self.write_meta_to_json(metadata_path, ucrt_dict)
        log.debug(f'uncertainties were written to {metadata_path}.')

    @staticmethod
    def write_meta_to_json(path: Path, ucrt_dict: Dict) -> None:
        path.parent.mkdir(parents=True, exist_ok=True)
        with open(path, 'w') as f:
            json.dump(ucrt_dict, f)

    def get_ucrt(self, calval: ET.Element, term: str, ucrt_dict: Dict) -> None:
        # get Uncertainties
        ucrts = calval.findall("Uncertainty")
        for ucrt in ucrts:
            name = ucrt.find("Name").text
            value = ucrt.find("Value").text
            if name not in list(self.uct_term_map.keys()):
                continue
            field = self.uct_term_map[name]
            if ucrt_dict.get(field):
                ucrt_dict.get(field)[term] = value
            else:
                ucrt_dict[field] = {term: value}

    def get_cal_coef(self, calval: ET.Element, coeff_dict: Dict) -> None:
        # get Calibration Coefficients
        coeffs = calval.findall("CalibrationCoefficient")
        for coeff in coeffs:
            name = coeff.find("Name").text
            if name not in self.cal_term_map:
                continue
            term = self.cal_term_map[name]
            coeff_dict[term] = coeff.find("Value").text

    def get_data_filename(self, path: Path) -> str:
        loc = os.path.basename(path)
        dd = os.path.basename(os.path.dirname(path))
        mm = os.path.basename(os.path.dirname(os.path.dirname(path)))
        yyyy = os.path.basename(os.path.dirname(os.path.dirname(os.path.dirname(path))))
        self.day = yyyy + '-' + mm + '-' + dd
        part_name = 'gascylinder_' + loc + '_' + self.day
        return part_name


def main() -> None:

    # terms with calibration
    uct_term_map = {'U_CVALA1': 'ucrtRaw', 'U_CVALA2': 'sd', 'U_CVALA3': 'ucrtAve',
                    'U_CVALD1': 'dfUcrtRaw', 'U_CVALD2': 'dfSd', 'U_CVALD3': 'dfUcrtAve'}
    cal_term_map = {'CVALC0': 'rtioMoleDryCo2Refe', 'CVALD0': 'dlta13CCo2Refe',
                    'CVALA0': 'rtioMoleDry12CCo2Refe', 'CVALB0': 'rtioMoleDry13CCo2Refe'}

    gas = GasCylinder(cal_term_map=cal_term_map, uct_term_map=uct_term_map)
    gas.l0tol0p()

    log.debug("done.")


if __name__ == "__main__":
    main()
