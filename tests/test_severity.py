from unittest.mock import patch

from pgreviewer.core.models import Severity
from pgreviewer.core.severity import classify_cost, classify_seq_scan


def test_classify_seq_scan_thresholds():
    with patch("pgreviewer.core.severity.settings") as mock_settings:
        mock_settings.SEQ_SCAN_ROW_THRESHOLD = 10_000
        mock_settings.SEQ_SCAN_CRITICAL_THRESHOLD = 1_000_000

        # Below warning
        assert classify_seq_scan(5_000) == Severity.INFO
        assert classify_seq_scan(10_000) == Severity.INFO

        # Above warning, below critical
        assert classify_seq_scan(50_000) == Severity.WARNING
        assert classify_seq_scan(1_000_000) == Severity.WARNING

        # Above critical
        assert classify_seq_scan(1_000_001) == Severity.CRITICAL


def test_classify_seq_scan_thresholds_configurable():
    with patch("pgreviewer.core.severity.settings") as mock_settings:
        # Change thresholds to prove that the classification reacts to config
        mock_settings.SEQ_SCAN_ROW_THRESHOLD = 500
        mock_settings.SEQ_SCAN_CRITICAL_THRESHOLD = 5000

        assert classify_seq_scan(400) == Severity.INFO
        assert classify_seq_scan(1000) == Severity.WARNING
        assert classify_seq_scan(10000) == Severity.CRITICAL


def test_classify_cost_thresholds():
    with patch("pgreviewer.core.severity.settings") as mock_settings:
        mock_settings.HIGH_COST_THRESHOLD = 10_000.0
        mock_settings.HIGH_COST_CRITICAL_THRESHOLD = 100_000.0

        # Below warning
        assert classify_cost(5_000.0) == Severity.INFO
        assert classify_cost(10_000.0) == Severity.INFO

        # Above warning, below critical
        assert classify_cost(50_000.0) == Severity.WARNING
        assert classify_cost(100_000.0) == Severity.WARNING

        # Above critical
        assert classify_cost(100_000.1) == Severity.CRITICAL


def test_classify_cost_thresholds_configurable():
    with patch("pgreviewer.core.severity.settings") as mock_settings:
        # Change thresholds to prove that the classification reacts to config
        mock_settings.HIGH_COST_THRESHOLD = 50.0
        mock_settings.HIGH_COST_CRITICAL_THRESHOLD = 500.0

        assert classify_cost(40.0) == Severity.INFO
        assert classify_cost(100.0) == Severity.WARNING
        assert classify_cost(1000.0) == Severity.CRITICAL
