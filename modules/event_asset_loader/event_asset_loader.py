from pathlib import Path
import structlog

log = structlog.get_logger()


class EventAssetLoader(object):

    def __init__(self, *, source_path: Path, out_path: Path, source_type_index: int, source_id_index: int):
        """
        Constructor.

        :param source_path: The data path.
        :param out_path: The output path for writing results.
        :param source_type_index: The file path source type index.
        :param source_id_index: The file path source ID index.
        """
        self.source_path = source_path
        self.out_path = out_path
        self.source_type_index = source_type_index
        self.source_id_index = source_id_index

    def link_event_files(self):
        if self.source_path.is_file():
            self.link_file(self.source_path)
        else:
            for path in self.source_path.rglob('*'):
                if path.is_file():
                    self.link_file(path)

    def link_file(self, path: Path):
        parts = path.parts
        source_type = parts[self.source_type_index]
        source_id = parts[self.source_id_index]
        log.debug(f'file: {path.name} type: {source_type} source_id: {source_id}')
        link_filename = f'{source_type}_{source_id}_events.json'
        link_path = Path(self.out_path, source_type, source_id, link_filename)
        log.debug(f'link_path: {link_path}')
        link_path.parent.mkdir(parents=True, exist_ok=True)
        link_path.symlink_to(path)
