from contextlib import closing

from pachyderm_sdk import Client

from processed_datums_reader.db_connector import Db
from processed_datums_reader.reader import read_processed_files
from processed_datums_reader.writer import write_to_db


def run(client: Client, db: Db) -> None:
    """Read the error files and write the metadata to the database."""
    files_by_pipeline = read_processed_files(client)
    with closing(db.connection):
        write_to_db(db, files_by_pipeline)
