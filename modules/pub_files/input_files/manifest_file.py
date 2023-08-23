import csv
import hashlib
import os
from pathlib import Path
from typing import List, Optional

from pub_files.output_files.science_review.science_review_file import ScienceReviewFile


class Visibility:
    """Class representing options for dataset visibility."""
    public = 'public'
    private = 'private'


class ManifestFile:
    """Class representing the manifest file."""

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
        self.manifest = self._read_existing_manifest()
        self.column_names = list(self.manifest[0].keys())
        self._remove_files()
        self.visibility = self._get_visibility()

    def add_metadata_files(self, variables_file: Path, positions_file: Path, eml_file: Path, readme_file: Path,
                           science_review_file: Optional[ScienceReviewFile]) -> None:
        """Add the metadata files to the manifest."""
        has_data = 'False'
        self._add_row(variables_file.name,
                      has_data,
                      self.visibility,
                      str(os.path.getsize(variables_file)),
                      self._get_md5_hash(variables_file))
        self._add_row(positions_file.name,
                      has_data,
                      self.visibility,
                      str(os.path.getsize(positions_file)),
                      self._get_md5_hash(positions_file))
        self._add_row(eml_file.name,
                      has_data,
                      self.visibility,
                      str(os.path.getsize(eml_file)),
                      self._get_md5_hash(eml_file))
        self._add_row(readme_file.name,
                      has_data,
                      self.visibility,
                      str(os.path.getsize(readme_file)),
                      self._get_md5_hash(readme_file))
        if science_review_file is not None:
            self._add_row(science_review_file.path.name,
                          has_data,
                          self.visibility,
                          str(os.path.getsize(science_review_file.path)),
                          self._get_md5_hash(science_review_file.path))

    def write_new_manifest(self) -> None:
        """Write a new manifest file to the output path."""
        path = Path(self.output_path, self.get_filename())
        with open(path, 'w', newline='') as csvfile:
            fieldnames = list(self.manifest[0].keys())
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
            writer.writeheader()
            for row in self.manifest:
                writer.writerow(row)

    def _add_row(self, filename: str, has_data: str, visibility: str, size: str, checksum: str) -> None:
        """Add a new row to the manifest."""
        row = {
            self.column_names[0]: filename,
            self.column_names[1]: has_data,
            self.column_names[2]: visibility,
            self.column_names[3]: size,
            self.column_names[4]: checksum
        }
        self.manifest.append(row)

    def _read_existing_manifest(self) -> List[dict]:
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

    def _remove_files(self) -> None:
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

    @staticmethod
    def get_filename() -> str:
        return 'manifest.csv'
