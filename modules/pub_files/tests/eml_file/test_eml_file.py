from datetime import datetime
from pathlib import Path
from typing import List, Iterator

from pyfakefs.fake_filesystem_unittest import TestCase

import common.date_formatter
from data_access.types.threshold import Threshold
from pub_files.database.named_locations import NamedLocation
from pub_files.database.publication_workbook import PublicationWorkbook
from pub_files.database.units import EmlUnitType
from pub_files.database.value_list import Value
from pub_files.geometry import Geometry, build_geometry
from pub_files.input_files.file_metadata import PathElements, FileMetadata, DataFiles, DataFile
from pub_files.input_files.manifest_file import ManifestFile
from pub_files.main import get_timestamp
from pub_files.output_files.eml.eml_database import EmlDatabase
from pub_files.output_files.eml.eml_file import EmlFileConfig, write_eml_file
from pub_files.output_files.eml.external_eml_files import ExternalEmlFiles
from pub_files.tests.input_file_processor_data.file_processor_database import get_data_product
from pub_files.tests.publication_workbook.publication_workbook import get_workbook


def get_encoding() -> str:
    return 'utf-8'


def get_named_location(_name):
    return NamedLocation(
        location_id=138773,
         name='CFGLOC101775',
         description='Central Plains Soil Temp Profile SP2, Z6 Depth',
         properties=[]
    )


def get_geometry(_name) -> Geometry:
    return build_geometry(
        geometry='POINT Z (-104.745591 40.815536 1653.9151)',
        srid=4979
    )


def get_unit_eml_type(_unit: str) -> EmlUnitType:
    return EmlUnitType(EmlUnitType.custom)


def get_spatial_unit(_srid: int) -> str:
    return 'meter'


def get_value_list(_list_name: str) -> List[Value]:
    return [
        Value(
            id=1,
            list_code='list_code',
            name='value name',
            rank=1,
            code='code',
            effective_date=get_timestamp(),
            end_date=get_timestamp(),
            publication_code='basic',
            description='A test value for the list.'
        )
    ]


def get_thresholds(_term_name: str) -> Iterator[Threshold]:
    yield Threshold(
        threshold_name='threshold_name',
        term_name='term_name',
        location_name='location_name',
        context=['context_1', 'context_2', 'context_3'],
        start_date=common.date_formatter.to_string(get_timestamp()),
        end_date=common.date_formatter.to_string(get_timestamp()),
        is_date_constrained=False,
        start_day_of_year=100,
        end_day_of_year=200,
        number_value=100,
        string_value='string_value'
    )


def get_data_files() -> DataFiles:
    now = datetime.now()
    name = 'NEON.D10.CPER.DP1.00041.001.210.000.000.prt.2020-03.expanded.20230405T190704Z.csv'
    data_file = DataFile(
        filename=name,
        description='A data file with data.',
        line_count=15,
        data_product_name='NEON.D10.CPER.DP1.00041.001.210.000.000'
    )
    return DataFiles(files=[data_file], min_time=now, max_time=now)


def get_database() -> EmlDatabase:
    return EmlDatabase(
        get_geometry=get_geometry,
        get_named_location=get_named_location,
        get_spatial_unit=get_spatial_unit,
        get_thresholds=get_thresholds,
        get_unit_eml_type=get_unit_eml_type,
        get_value_list=get_value_list
    )


class EmlTest(TestCase):

    def setUp(self) -> None:
        # Must execute get_workbook() before pyfakefs is set up.
        self.workbook: PublicationWorkbook = get_workbook('')
        self.setUpPyfakefs()
        self.base_file_path = Path(__file__).parent.resolve()
        self.domain = 'D10'
        self.site = 'CPER'
        self.year = '2020'
        self.month = '01'
        self.out_path = Path(
            '/out',
            self.domain,
            self.site,
            self.year,
            self.month,
            'basic'
        )
        self.fs.create_dir(self.out_path)
        self.data_product_id = 'DP1.00041.01'
        self.boilerplate_text: str = self.read_boilerplate_file()
        self.contact_text: str = self.read_contact_file()
        self.intellectual_rights_text: str = self.read_intellectual_rights_file()
        self.unit_types_text: str = self.read_unit_types_file()
        self.units_text: str = self.read_units_file()
        self.manifest_path: Path = self.get_manifest_path()

    def test_write_file(self):
        eml_file_config = EmlFileConfig(
            out_path=self.out_path,
            metadata=self.get_file_metadata(),
            eml_templates=self.get_external_files(),
            timestamp=get_timestamp(),
            database=get_database(),
            workbook=self.workbook,
            package_type='basic'
        )
        eml_file_path = write_eml_file(eml_file_config)
        print(f'\ncontent:\n\n{eml_file_path.read_text(encoding=get_encoding())}\n\n')
        assert eml_file_path.exists()

    def read_boilerplate_file(self) -> str:
        real_path = Path(self.base_file_path, 'boilerplate.xml')
        boilerplate_path = Path('/boilerplate.xml')
        self.fs.add_real_file(real_path, target_path=boilerplate_path)
        return boilerplate_path.read_text(get_encoding())

    def read_contact_file(self) -> str:
        real_path = Path(self.base_file_path, 'contact.xml')
        contact_path = Path('/contact.xml')
        self.fs.add_real_file(real_path, target_path=contact_path)
        return contact_path.read_text(get_encoding())

    def read_intellectual_rights_file(self) -> str:
        real_path = Path(self.base_file_path, 'intellectual_rights.xml')
        intellectual_rights_path = Path('/intellectual_rights.xml')
        self.fs.add_real_file(real_path, target_path=intellectual_rights_path)
        return intellectual_rights_path.read_text(get_encoding())

    def read_unit_types_file(self) -> str:
        real_path = Path(self.base_file_path, 'unit_types.xml')
        unit_types_path = Path('/unit_types.xml')
        self.fs.add_real_file(real_path, target_path=unit_types_path)
        return unit_types_path.read_text(get_encoding())

    def read_units_file(self) -> str:
        real_path = Path(self.base_file_path, 'units.csv')
        units_path = Path('/units.csv')
        self.fs.add_real_file(real_path, target_path=units_path)
        return units_path.read_text(get_encoding())

    def get_manifest_path(self) -> Path:
        real_path = Path(self.base_file_path, 'manifest.csv')
        manifest_path = Path('/manifest.csv')
        self.fs.add_real_file(real_path, target_path=manifest_path)
        return manifest_path

    def get_file_metadata(self) -> FileMetadata:
        elements = PathElements(
            domain=self.domain,
            site=self.site,
            year=self.year,
            month=self.month,
            data_product_id=self.data_product_id
        )
        data_product = get_data_product(_data_product_id='unused')
        file_metadata = FileMetadata()
        file_metadata.path_elements = elements
        file_metadata.data_files = get_data_files()
        file_metadata.data_product = data_product
        file_metadata.manifest_file = ManifestFile(
            self.manifest_path,
            'basic',
            self.out_path
        )
        file_metadata.package_output_path = Path(self.out_path)
        return file_metadata

    def get_external_files(self) -> ExternalEmlFiles:
        return ExternalEmlFiles(
            boilerplate=self.boilerplate_text,
            contact=self.contact_text,
            intellectual_rights=self.intellectual_rights_text,
            unit_types=self.unit_types_text,
            units=self.units_text
        )
