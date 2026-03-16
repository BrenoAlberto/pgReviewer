from dataclasses import dataclass, field

from unidiff import PatchSet


@dataclass
class ChangedFile:
    path: str
    added_lines: list[str] = field(default_factory=list)
    added_line_numbers: list[int] = field(default_factory=list)
    is_new_file: bool = False


def parse_diff(diff_str: str) -> list[ChangedFile]:
    """Parse a unified diff string and return changed files with added lines only.

    Args:
        diff_str: A unified diff string (e.g. from ``git diff`` or a ``.patch`` file).

    Returns:
        A list of :class:`ChangedFile` instances, one per modified file.
        Only addition lines are captured; deletions are ignored.
    """
    patch = PatchSet(diff_str)
    result: list[ChangedFile] = []

    for patched_file in patch:
        added_lines: list[str] = []
        added_line_numbers: list[int] = []

        for hunk in patched_file:
            for line in hunk:
                if line.is_added:
                    added_lines.append(line.value.rstrip("\n"))
                    added_line_numbers.append(line.target_line_no)

        result.append(
            ChangedFile(
                path=patched_file.path,
                added_lines=added_lines,
                added_line_numbers=added_line_numbers,
                is_new_file=patched_file.is_added_file,
            )
        )

    return result
