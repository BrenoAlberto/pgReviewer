from pgreviewer.analysis.code_pattern_detectors.python.query_in_loop import (
    QueryInLoopDetector,
)
from pgreviewer.analysis.code_pattern_detectors.python.sql_injection_fstring import (
    FStringInjectDetector,
)
from pgreviewer.analysis.code_pattern_detectors.python.sqlalchemy_n_plus_one import (
    SQLAlchemyNPlusOneDetector,
)

__all__ = [
    "FStringInjectDetector",
    "QueryInLoopDetector",
    "SQLAlchemyNPlusOneDetector",
]
