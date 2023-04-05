import unittest

from pub_files.tests.publication_workbook.publication_workbook import get_workbook


class PublicationWorkbookTest(unittest.TestCase):

    @staticmethod
    def test():
        workbook = get_workbook()
        print(f'workbook:\n{workbook}\n')
