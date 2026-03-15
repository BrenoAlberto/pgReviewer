from pgreviewer.config import settings
from pgreviewer.core.models import Severity


def classify_seq_scan(row_estimate: int) -> Severity:
    """Classifies a sequential scan based on row estimate and config thresholds."""
    if row_estimate > settings.SEQ_SCAN_CRITICAL_THRESHOLD:
        return Severity.CRITICAL
    elif row_estimate > settings.SEQ_SCAN_ROW_THRESHOLD:
        return Severity.WARNING
    else:
        return Severity.INFO


def classify_cost(total_cost: float) -> Severity:
    """Classifies a query plan based on total cost and config thresholds."""
    if total_cost > settings.HIGH_COST_CRITICAL_THRESHOLD:
        return Severity.CRITICAL
    elif total_cost > settings.HIGH_COST_THRESHOLD:
        return Severity.WARNING
    else:
        return Severity.INFO
