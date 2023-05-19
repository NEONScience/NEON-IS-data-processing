from typing import NamedTuple, List

class PubWorkbookRow(NamedTuple):
    """Class to represent a single row in a publication workbook."""
    rank: int
    DPName: str
    dpID: str
    dpIDSource: str
    DPNumber: str
    table: str
    tableDescription: str
    fieldName: str
    description: str
    dataType: str
    units: str
    measurementScale: str
    ontologyMapping: str
    pubFormat: str
    exampleEntry: str
    usage: str
    fieldType: str
    tableType: str
    inputs: str
    #ingestDPIDSource: str
    #ingestTableSource: str
    #ingestFieldSource: str
    #inputFieldParameter: str
    filterSampleClass: str
    timeIndex: str
    timeDescription: str
    downloadPkg: str
    dataCategory: str
    sampleInfo: str
    lovName: str
    primaryKey: str
    redactionFlag: str

class PubWorkbook:

    def __init__(self, workbook_rows: List[PubWorkbookRow]):
        self.workbook_rows = workbook_rows
