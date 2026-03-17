from pgreviewer.config import settings
from pgreviewer.core.models import Issue, Severity


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


def apply_rule_severity_overrides(
    issues: list[Issue],
    rules: dict[str, object],
) -> list[Issue]:
    for issue in issues:
        rule = rules.get(issue.detector_name)
        if rule is None:
            continue
        severity = getattr(rule, "severity", None)
        if severity is None:
            continue
        try:
            issue.severity = Severity[str(severity).upper()]
        except ValueError:
            continue
    return issues
