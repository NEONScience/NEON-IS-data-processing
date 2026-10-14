import csv
import hashlib
from pathlib import Path
from tempfile import TemporaryDirectory
from unittest import TestCase

from pub_files.input_files.manifest_file import ManifestFile, Visibility
from pub_files.output_files.science_review.science_review_file import ScienceReviewFile


class ManifestFileTest(TestCase):

    columns = ['file', 'hasData', 'visibility', 'size', 'checksum']

    def setUp(self):
        self.temp_dir = TemporaryDirectory()
        self.root = Path(self.temp_dir.name)
        self.source_path = self.root / 'source' / 'manifest.csv'
        self.output_path = self.root / 'output'
        self.source_path.parent.mkdir()
        self.output_path.mkdir()
        self.manifest_file = self.create_manifest_file(self._source_rows())

    def tearDown(self):
        self.temp_dir.cleanup()

    def test_init_filters_other_packages_and_keeps_metadata_files(self):
        filenames = [row['file'] for row in self.manifest_file.manifest]

        self.assertEqual(filenames, [
            'product.basic.csv',
            'product.EML.xml',
            'product.variables.csv',
            'product.readme.txt',
            'product.science_review_flags.csv',
            'product.sensor_positions.csv',
        ])
        self.assertEqual(self.manifest_file.visibility, Visibility.public)

    def test_visibility_is_private_when_no_retained_row_is_public(self):
        row = self._row('product.basic.csv', visibility=Visibility.private)
        manifest_file = self.create_manifest_file([row])

        self.assertEqual(manifest_file.visibility, Visibility.private)

    def test_add_file_records_size_checksum_and_visibility(self):
        contents = b'data,value\r\n1,2\r\n'
        data_path = self._create_file('product.basic.csv', contents)

        self.manifest_file.add_file(data_path, has_data=True)

        self.assertEqual(self.manifest_file.manifest[-1], {
            'file': data_path.name,
            'hasData': 'True',
            'visibility': Visibility.public,
            'size': str(len(contents)),
            'checksum': hashlib.md5(contents).hexdigest(),
        })

    def test_add_metadata_files_records_all_metadata_checksums(self):
        metadata_paths = self._create_metadata_files()
        science_review_path = self._create_file('product.science_review_flags.csv', b'flag\r\none\r\n')
        science_review_file = ScienceReviewFile(
            path=science_review_path,
            data_product_id='DP1.00001.01',
            variables=[],
        )

        self.manifest_file.add_metadata_files(
            variables_file=metadata_paths['variables'],
            positions_file=metadata_paths['positions'],
            eml_file=metadata_paths['eml'],
            readme_file=metadata_paths['readme'],
            science_review_file=science_review_file,
        )

        rows = self.manifest_file.manifest[-5:]
        expected_paths = [*metadata_paths.values(), science_review_path]
        self.assertEqual([row['file'] for row in rows], [path.name for path in expected_paths])
        for row, path in zip(rows, expected_paths):
            contents = path.read_bytes()
            self.assertEqual(row, {
                'file': path.name,
                'hasData': 'False',
                'visibility': Visibility.public,
                'size': str(len(contents)),
                'checksum': hashlib.md5(contents).hexdigest(),
            })

    def test_add_metadata_files_without_science_review(self):
        metadata_paths = self._create_metadata_files()

        self.manifest_file.add_metadata_files(
            variables_file=metadata_paths['variables'],
            positions_file=metadata_paths['positions'],
            eml_file=metadata_paths['eml'],
            readme_file=metadata_paths['readme'],
            science_review_file=None,
        )

        self.assertEqual(
            [row['file'] for row in self.manifest_file.manifest[-4:]],
            [path.name for path in metadata_paths.values()],
        )

    def test_write_new_manifest_serializes_header_and_rows(self):
        self.manifest_file.write_new_manifest()
        written_path = self.output_path / ManifestFile.get_filename()

        with written_path.open(newline='') as manifest_csv:
            reader = csv.DictReader(manifest_csv)
            rows = list(reader)

        self.assertEqual(reader.fieldnames, self.columns)
        self.assertEqual(rows, self.manifest_file.manifest)

    def test_get_filename(self):
        self.assertEqual(ManifestFile.get_filename(), 'manifest.csv')

    def test_md5_hash_preserves_file_bytes(self):
        contents = b'field,value\r\none,two\r\n'
        path = self._create_file('metadata.csv', contents)

        self.assertEqual(ManifestFile._get_md5_hash(path), hashlib.md5(contents).hexdigest())

    def create_manifest_file(self, rows):
        with self.source_path.open('w', newline='') as manifest_csv:
            writer = csv.DictWriter(manifest_csv, fieldnames=self.columns)
            writer.writeheader()
            writer.writerows(rows)
        return ManifestFile(self.source_path, 'basic', self.output_path)

    def _create_metadata_files(self):
        return {
            'variables': self._create_file('product.variables.csv', b'field\r\nvalue\r\n'),
            'positions': self._create_file('product.sensor_positions.csv', b'position\r\nA\r\n'),
            'eml': self._create_file('product.EML.xml', b'<eml/>'),
            'readme': self._create_file('product.readme.txt', b'readme'),
        }

    def _create_file(self, name, contents):
        path = self.root / name
        path.write_bytes(contents)
        return path

    def _source_rows(self):
        return [
            self._row('product.basic.csv', visibility=Visibility.public),
            self._row('product.expanded.csv'),
            self._row('unrelated-first.csv'),
            self._row('unrelated-second.csv'),
            self._row('product.EML.xml'),
            self._row('product.variables.csv'),
            self._row('product.readme.txt'),
            self._row('product.science_review_flags.csv'),
            self._row('product.sensor_positions.csv'),
        ]

    def _row(self, filename, visibility=Visibility.private):
        return {
            'file': filename,
            'hasData': 'True',
            'visibility': visibility,
            'size': '1',
            'checksum': 'checksum',
        }