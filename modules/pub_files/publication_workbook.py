import csv
from io import StringIO
from typing import List, Dict

from pub_files.input_files.filename_parser import FilenameData


class PublicationWorkbook:

    def __init__(self, publication_workbook: str):
        self.publication_workbook: List[dict] = self.load(publication_workbook)
        self.file_descriptions = self.load_descriptions()

    def get_workbook(self):
        return self.publication_workbook

    @staticmethod
    def get_download_package(row: dict) -> str:
        return row['downloadPkg']

    @staticmethod
    def get_field_name(row: dict) -> str:
        return row['fieldName']

    @staticmethod
    def get_table_description(row: dict) -> str:
        return row['tableDescription']

    @staticmethod
    def get_measurement_scale(row: dict) -> str:
        return row['measurementScale']

    @staticmethod
    def get_os_lov(row: dict) -> str:
        return row['lovName']

    @staticmethod
    def get_data_type(row: dict) -> str:
        return row['dataType']

    @staticmethod
    def get_unit(row: dict) -> str:
        return row['units']

    @staticmethod
    def get_publication_format(row: dict) -> str:
        return row['pubFormat']

    def get_file_description(self, filename_data: FilenameData) -> str:
        key = self.make_key(filename_data.table_name, filename_data.download_package)
        return self.file_descriptions[key]

    @staticmethod
    def make_key(table_name: str, download_package: str) -> str:
        return f'{table_name}.{download_package}'

    @staticmethod
    def remove_comments(csv_file: StringIO):
        for row in csv_file:
            raw = row.split('"<!--')[0].strip()
            if raw: yield raw

    # TODO: Use this in variables_file.py as well.
    def load(self, workbook: str) -> List[dict]:
        string_io = StringIO(workbook, newline='\n')
        reader = csv.DictReader(self.remove_comments(string_io), delimiter='\t')
        return list(reader)

    def load_descriptions(self) -> Dict[str, str]:
        file_descriptions = {}
        for row in self.publication_workbook:
            try:
                table_name = row['name']
            except KeyError:
                table_name = row['table']
            download_package = self.get_download_package(row)
            key = self.make_key(table_name, download_package)
            description = self.get_table_description(row)
            file_descriptions[key] = description
        return file_descriptions
