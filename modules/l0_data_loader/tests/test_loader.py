#!/usr/bin/env python3
import os
import unittest
# from pathlib import Path
# import xml.etree.ElementTree as ElementTree

from pyfakefs.fake_filesystem_unittest import TestCase
import psycopg2
from contextlib import closing

from calval_loader.get_calibration_stream_name import get_calibration_stream_name


# from certificate_file_parser import CertificateFileParser
# from get_calibration_stream_name import get_calibration_stream_name
# import s3_client

@unittest.skip('Is this module used?')
class AppTest(TestCase):

    def setUp(self):
        self.setUpPyfakefs()

    # def test_s3_download(self):
    #     out_path = Path('/out')
    #     self.fs.create_dir(out_path)
    #     bucket = s3_client.get_bucket()
    #     file_name = '10000000005118_WO29435_164027.xml'
    #     expected_path = Path(out_path, file_name)
    #     bucket.download_file(file_name, str(expected_path))
    #     assert expected_path.exists()

    # def test_certificate_parser(self):
    #     test_file_path = Path(os.path.dirname(__file__), '10000000000084_WO21814_120104.xml')
    #     calibration_path = '/calibrations.xml'
    #     self.fs.add_real_file(test_file_path, target_path=calibration_path)
    #     xml_root = ElementTree.parse(calibration_path).getroot()
    #     parser = CertificateFileParser()
    #     certificate = parser.parse(xml_root)
    #     certificate.show()
    #     assert certificate is not None

    @staticmethod
    def test_calibration_stream_name():
        # database URL in the form: [user]/[pass]@[url]:[port]/[sid]
        db_url = os.getenv('DATABASE_URL')
        with closing(psycopg2.connect(db_url)) as connection:
            stream_name = get_calibration_stream_name(connection, 'exo2', 0)
            assert stream_name == 'conductance'
