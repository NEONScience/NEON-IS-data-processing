import csv
from io import StringIO
from typing import List, Dict

from pub_files.output_files.eml.stmml.stmml_1_2 import stmml


class NeonUnits:
    """Class to parse the NEON units csv file."""

    def __init__(self, neon_units_file_content: str):
        self.units: List[dict] = self._parse_units(neon_units_file_content)
        self.units_by_name = self._organize_by_name()

    def to_stmml(self, unit_name: str) -> stmml.Unit:
        """Convert the unit to Scientific Technological Medical Metadata Language (STMML) format."""
        row = self.units_by_name[unit_name]
        unit = stmml.Unit()
        unit.name = unit_name
        unit.id = unit_name
        unit.abbreviation = row['unitAbbr']
        unit.constant_to_si = row['constantToSI']
        unit.multiplier_to_si = row['multiplierToSI']
        unit.parent_si = row['parentSI']
        unit.unit_type = row['unitType']
        return unit

    def _organize_by_name(self) -> Dict[str, dict]:
        """Return a dictionary of units by unit name."""
        units_by_name = {}
        for row in self.units:
            unit_name = row['unitName']
            units_by_name[unit_name] = row
        return units_by_name

    @staticmethod
    def _parse_units(neon_units_file_content: str) -> List[dict]:
        """Parse the NEON units CSV file into a list of dictionaries representing the file rows."""
        string_io = StringIO(neon_units_file_content, newline='\n')
        reader = csv.DictReader(string_io, delimiter='\t')
        return list(reader)
