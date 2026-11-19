import shutil
import tempfile
from pathlib import Path
from unittest import TestCase

import yaml

from filter_joiner.joiner import FilterJoiner
from replace_sn_with_assetuid.replace_sn_with_assetuid import load_assetuid


class DateControlTest(TestCase):

    def setUp(self):
        self.temp_dir = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp_dir.cleanup)
        self.root = Path(self.temp_dir.name)
        pipeline_path = (
            Path(__file__).resolve().parents[3]
            / 'pipe/exo2/exo2_replace_sn_with_assetuid.yaml'
        )
        self.pipeline = yaml.safe_load(pipeline_path.read_text())
        self.config = yaml.safe_load(self.pipeline['transform']['env']['CONFIG_LIMIT_LOGFILES'])
        self.relative_path_index = len((self.root / 'DATA_PATH').parts)
        offset = self.relative_path_index - 3
        for item in self.config['input_paths']:
            path = item['path']
            self.assertEqual(path['join_indices'], [3, 4, 5, 6])
            self.assertFalse(path['outer_join'])
            path['glob_pattern'] = path['glob_pattern'].replace('/pfs/', f'{self.root}/')
            path['join_indices'] = [index + offset for index in path['join_indices']]

    def write_file(self, relative_path, contents):
        path = self.root / relative_path
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(contents)
        return path

    def run_filter(self):
        limited = self.root / 'log_limited'
        limited.mkdir()
        FilterJoiner(
            config=yaml.safe_dump(self.config),
            out_path=limited,
            relative_path_index=self.relative_path_index,
            symlink=True,
        ).join()
        return limited

    def test_matching_dates_and_persistent_asset_output(self):
        for day in ('02', '12'):
            self.write_file(f'DATA_PATH/exo2/2025/09/{day}/SN123/data/log.parquet', day)
            (self.root / f'DATE_CONTROL/exo2/2025/09/{day}').mkdir(parents=True)
        for day in ('01', '08', '13'):
            self.write_file(f'DATA_PATH/exo2/2025/09/{day}/SN123/data/log.parquet', day)
        self.write_file('DATA_PATH/exo2conductivity/2025/09/02/SN123/data/log.parquet', 'wrong sensor')
        self.write_file('MAP_PATH/exo2/SN123/asset.txt', '43764')
        limited = self.run_filter()
        staged = self.root / 'sn_replaced'
        load_assetuid(limited, self.root / 'MAP_PATH', staged, self.relative_path_index)
        output = self.root / 'out'
        shutil.copytree(staged, output, symlinks=False)
        shutil.rmtree(limited)
        shutil.rmtree(staged)
        self.assertEqual(
            sorted(str(path.relative_to(output)) for path in output.rglob('*.parquet')),
            [f'exo2/2025/09/{day}/43764/data/log.parquet' for day in ('02', '12')],
        )
        for day in ('02', '12'):
            path = output / f'exo2/2025/09/{day}/43764/data/log.parquet'
            self.assertFalse(path.is_symlink())
            self.assertEqual(path.read_text(), day)

    def test_no_matching_dates(self):
        self.write_file('DATA_PATH/exo2/2025/09/01/SN123/data/log.parquet', 'outside')
        (self.root / 'DATE_CONTROL/exo2/2025/09/02').mkdir(parents=True)
        limited = self.run_filter()
        self.assertEqual(list(limited.iterdir()), [])
