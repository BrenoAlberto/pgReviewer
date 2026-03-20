"""Tests for pgreviewer.ci.pg_version_detector."""

from __future__ import annotations

from pathlib import Path

from pgreviewer.ci.pg_version_detector import DEFAULT_PG_VERSION, detect, detect_or_default


def _write(tmp_path: Path, filename: str, content: str) -> None:
    (tmp_path / filename).write_text(content)


# ── docker-compose detection ──────────────────────────────────────────────────


def test_detects_version_from_docker_compose_yml(tmp_path):
    _write(tmp_path, "docker-compose.yml", "image: postgres:15\n")
    assert detect(tmp_path) == 15


def test_detects_version_from_docker_compose_yaml(tmp_path):
    _write(tmp_path, "docker-compose.yaml", "image: postgres:14\n")
    assert detect(tmp_path) == 14


def test_detects_major_version_only_from_full_tag(tmp_path):
    _write(tmp_path, "docker-compose.yml", "image: postgres:16.3-bullseye\n")
    assert detect(tmp_path) == 16


def test_ignores_postgres_latest_tag(tmp_path):
    _write(tmp_path, "docker-compose.yml", "image: postgres:latest\n")
    assert detect(tmp_path) is None


# ── dockerfile detection ──────────────────────────────────────────────────────


def test_detects_version_from_dockerfile(tmp_path):
    _write(tmp_path, "Dockerfile", "FROM postgres:17\n")
    assert detect(tmp_path) == 17


def test_detects_version_from_dockerfile_with_alpine(tmp_path):
    _write(tmp_path, "Dockerfile", "FROM postgres:15-alpine\n")
    assert detect(tmp_path) == 15


def test_detects_version_from_namespaced_image(tmp_path):
    _write(tmp_path, "Dockerfile", "FROM library/postgres:14\n")
    assert detect(tmp_path) == 14


# ── precedence: docker-compose wins over Dockerfile ──────────────────────────


def test_docker_compose_takes_precedence_over_dockerfile(tmp_path):
    _write(tmp_path, "docker-compose.yml", "image: postgres:15\n")
    _write(tmp_path, "Dockerfile", "FROM postgres:14\n")
    assert detect(tmp_path) == 15


# ── no version found ─────────────────────────────────────────────────────────


def test_returns_none_when_no_postgres_reference_found(tmp_path):
    _write(tmp_path, "docker-compose.yml", "image: redis:7\n")
    assert detect(tmp_path) is None


def test_returns_none_for_empty_directory(tmp_path):
    assert detect(tmp_path) is None


# ── detect_or_default ─────────────────────────────────────────────────────────


def test_detect_or_default_returns_detected_version(tmp_path):
    _write(tmp_path, "docker-compose.yml", "image: postgres:14\n")
    version, was_detected = detect_or_default(tmp_path)
    assert version == 14
    assert was_detected is True


def test_detect_or_default_falls_back_to_default(tmp_path):
    version, was_detected = detect_or_default(tmp_path)
    assert version == DEFAULT_PG_VERSION
    assert was_detected is False
