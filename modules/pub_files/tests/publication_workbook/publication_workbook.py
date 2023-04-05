import os
from pathlib import Path


def get_workbook() -> str:
    file_path = Path(os.path.dirname(__file__), 'soil_temperature_publication_workbook.txt')
    return file_path.read_text()
