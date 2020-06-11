from pathlib import Path
import structlog

from date_gap_filler.empty_files import EmptyFiles

log = structlog.get_logger()


class EmptyFileLinker(object):
    """Class to link empty files."""

    def __init__(self, empty_files: EmptyFiles, location: str, year: str, month: str, day: str):
        self.empty_files = empty_files
        self.location = location
        self.year = year
        self.month = month
        self.day = day

    def link_data_file(self, out_path: Path):
        self.link_empty_file(out_path, self.empty_files.data_path)

    def link_flags_file(self, out_path: Path):
        self.link_empty_file(out_path, self.empty_files.flags_path)

    def link_uncertainty_data_file(self, out_path: Path):
        self.link_empty_file(out_path, self.empty_files.uncertainty_data_path)

    def link_empty_file(self, out_path: Path, file: Path):
        """
        Link the file into the output path.

        :param out_path: The target path for linking files.
        :param file: The source empty file path.
        """
        filename = file.name
        filename = filename.replace('location', self.location)
        filename = filename.replace('year', self.year)
        filename = filename.replace('month', self.month)
        filename = filename.replace('day', self.day)
        filename += '.empty'  # add extension to distinguish from real data files.
        link_path = Path(out_path, filename)
        log.debug(f'source: {file}, link: {link_path}')
        link_path.parent.mkdir(parents=True, exist_ok=True)
        if not link_path.exists():
            link_path.symlink_to(file)
