import csv
import hashlib
import os
from pathlib import Path
from typing import List

from pub_files.output_files.science_review.science_review_file import ScienceReviewFile


class Visibility:
    """Class representing options for dataset visibility."""
    public = 'public'
    private = 'private'


class ManifestFile:
    """Class representing the manifest file."""

    filename = 'manifest.csv'
    has_data = 'False'

    def __init__(self, manifest_file_path: Path, package_type: str, output_path: Path) -> None:
        """
        Constructor.

        :param manifest_file_path: The manifest file.
        :param package_type: The download package type for the manifest.
        :param output_path: The root output path for the writing the manifest.
        """
        self.manifest_file_path = manifest_file_path
        self.package_type = package_type
        self.output_path = output_path
        self.manifest = self._read()
        self.column_names = list(self.manifest[0].keys())
        self._remove_files()
        self.visibility = self._get_visibility()

    def add_metadata_files(self, variables_file: Path, positions_file: Path, eml_file: Path, readme_file: Path,
                           science_review_file: ScienceReviewFile) -> None:
        """Add the metadata files to the manifest."""
        variables_file_size = os.path.getsize(variables_file)
        positions_file_size = os.path.getsize(positions_file)
        eml_file_size = os.path.getsize(eml_file)
        readme_file_size = os.path.getsize(readme_file)
        self._add_row(variables_file.name,
                      self.has_data,
                      self.visibility,
                      str(variables_file_size),
                      self._get_md5_hash(variables_file))
        self._add_row(positions_file.name,
                      self.has_data,
                      self.visibility,
                      str(positions_file_size),
                      self._get_md5_hash(positions_file))
        self._add_row(eml_file.name,
                      self.has_data,
                      self.visibility,
                      str(eml_file_size),
                      self._get_md5_hash(eml_file))
        self._add_row(readme_file.name,
                      self.has_data,
                      self.visibility,
                      str(readme_file_size),
                      self._get_md5_hash(readme_file))
        if science_review_file.path:
            self._add_row(science_review_file.path.name,
                          self.has_data,
                          self.visibility,
                          str(os.path.getsize(science_review_file.path)),
                          self._get_md5_hash(science_review_file.path))

    def write(self) -> None:
        """Write a new manifest file to the output path."""
        path = Path(self.output_path, self.filename)
        with open(path, 'w', newline='') as csvfile:
            fieldnames = list(self.manifest[0].keys())
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
            writer.writeheader()
            for row in self.manifest:
                writer.writerow(row)

    def _add_row(self, filename: str, has_data: str, visibility: str, size: str, checksum: str):
        """Add a new row to the manifest."""
        row = {
            self.column_names[0]: filename,
            self.column_names[1]: has_data,
            self.column_names[2]: visibility,
            self.column_names[3]: size,
            self.column_names[4]: checksum
        }
        self.manifest.append(row)

    def _read(self) -> List[dict]:
        """Read the existing manifest."""
        with open(self.manifest_file_path, newline='') as csv_file:
            reader = csv.DictReader(csv_file)
            return list(reader)

    def _get_visibility(self) -> str:
        """Get the dataset visibility. If any data file is public the dataset is public."""
        for row in self.manifest:
            visibility: str = row[self.column_names[2]]
            if visibility.lower() == Visibility.public:
                return Visibility.public
        return Visibility.private

    def _remove_files(self):
        """Remove files without the package type."""
        for row in self.manifest:
            filename = row[self.column_names[0]]
            if self.package_type not in filename:
                self.manifest.remove(row)

    @staticmethod
    def _get_md5_hash(path: Path) -> str:
        """Create the md5 hash of a file."""
        with open(path) as f:
            data = f.read()
            return hashlib.md5(data.encode('utf-8')).hexdigest()
