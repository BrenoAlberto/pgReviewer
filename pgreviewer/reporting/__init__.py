"""Reporting helpers for user-facing outputs."""

from pgreviewer.reporting.cli_report import generate_cli_report
from pgreviewer.reporting.pr_comment import generate_pr_comment
from pgreviewer.reporting.sections import (
    ReportSection,
    SectionType,
    build_report_sections,
)

__all__ = [
    "ReportSection",
    "SectionType",
    "build_report_sections",
    "generate_cli_report",
    "generate_pr_comment",
]
