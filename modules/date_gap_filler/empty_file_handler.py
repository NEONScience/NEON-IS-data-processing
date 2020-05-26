from pathlib import Path
import structlog

log = structlog.get_logger()


class EmptyFiles(object):
    """Class to hold empty file paths."""

    def __init__(self, empty_files_path: Path, file_type_index: int):
        """
        Constructor.

        :param empty_files_path: The directory containing empty files.
        :param file_type_index: The index of the file type (e.g. 'flags') in the file path.
        """
        for path in empty_files_path.rglob('*'):
            if path.is_file():
                file_type = path.parts[file_type_index]
                if 'data' == file_type:
                    self.data_path = path
                elif 'flags' == file_type:
                    self.flags_path = path
                elif 'uncertainty_data' == file_type:
                    self.uncertainty_path = path


class EmptyFileLinker(object):
    """Class to handle linking empty files."""

    def __init__(self, empty_files: EmptyFiles, location: str, year: str, month: str, day: str):
        self.empty_files = empty_files
        self.location = location
        self.year = year
        self.month = month
        self.day = day

    def link_data_file(self, out_dir: Path):
        self.link_empty_file(out_dir, self.empty_files.data_path)

    def link_flags_file(self, out_dir: Path):
        self.link_empty_file(out_dir, self.empty_files.flags_path)

    def link_uncertainty_file(self, out_dir: Path):
        self.link_empty_file(out_dir, self.empty_files.uncertainty_path)

    def link_empty_file(self, output_dir: Path, file: Path):
        """
        Link the file into the output directory.

        :param output_dir: The target directory for linking files.
        :param file: The source empty file path.
        """
        filename = file.name
        filename = filename.replace('location', self.location)
        filename = filename.replace('year', self.year)
        filename = filename.replace('month', self.month)
        filename = filename.replace('day', self.day)
        filename += '.empty'  # add extension to distinguish from real data files.
        link_path = Path(output_dir, filename)
        link_path.parent.mkdir(parents=True, exist_ok=True)
        log.debug(f'source: {file}, link: {link_path}')
        link_path.symlink_to(file)
