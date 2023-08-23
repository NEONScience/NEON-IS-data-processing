import unittest

from pub_files.database.publication_workbook import PublicationWorkbook
from pub_files.tests.publication_workbook.publication_workbook import get_workbook


class PublicationWorkbookTest(unittest.TestCase):

    @staticmethod
    def test():
        workbook: PublicationWorkbook = get_workbook('')
        assert workbook is not None
        for row in workbook.rows:
            print(f'workbook row:\n{row}\n')
