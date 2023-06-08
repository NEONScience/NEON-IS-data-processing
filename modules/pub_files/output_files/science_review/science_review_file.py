from pathlib import Path
from typing import NamedTuple, Optional, List


class Term(NamedTuple):
    name: str
    number: str


class ScienceReviewFile(NamedTuple):
    path: Optional[Path]
    data_product_id: Optional[str]
    terms: Optional[List[Term]]
