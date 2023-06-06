from pathlib import Path
from typing import NamedTuple, Optional, List


class ScienceReviewFile(NamedTuple):
    path: Optional[Path]
    data_product_id: Optional[str]
    term_names: Optional[List[str]]
