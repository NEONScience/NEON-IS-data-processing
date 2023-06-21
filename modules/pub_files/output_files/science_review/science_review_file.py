from pathlib import Path
from typing import NamedTuple, Optional

from pub_files.database.file_variables import FileVariables


class Term(NamedTuple):
    name: str
    number: str


class ScienceReviewFile(NamedTuple):
    path: Optional[Path]
    data_product_id: Optional[str]
    variables: list[FileVariables]
