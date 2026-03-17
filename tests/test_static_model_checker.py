from pathlib import Path

from pgreviewer.analysis.static_model_checker import check_models_in_path

FIXTURE_DIR = Path(__file__).parent / "fixtures" / "models"


def test_static_model_checker_bad_model():
    path = FIXTURE_DIR / "fixture_bad_model.py"
    issues = check_models_in_path(path)

    detector_names = {i.detector_name for i in issues}

    assert "MissingTablename" in detector_names
    assert "MissingFKIndex" in detector_names
    assert "MissingCommonFilterIndex" in detector_names


def test_static_model_checker_good_model():
    path = FIXTURE_DIR / "fixture_good_model.py"
    issues = check_models_in_path(path)

    assert len(issues) == 0
