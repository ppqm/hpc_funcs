import tempfile
import weakref
from pathlib import Path
from typing import Any

FILENAMES = tempfile._get_candidate_names()  # type: ignore


def generate_name() -> str:
    """Generate a random name"""
    return next(FILENAMES)


class WorkDir(tempfile.TemporaryDirectory):
    """Like TemporaryDirectory, with the possiblity of keeping log files for debug"""

    def __init__(
        self,
        suffix: str | None = None,
        prefix: str | None = None,
        dir: str | Path | None = None,
        keep: bool = False,
    ) -> None:
        self.keep_directory = keep
        self.name = tempfile.mkdtemp(suffix, prefix, dir)
        if not keep:
            self._finalizer = weakref.finalize(
                self,
                super()._cleanup,  # type: ignore
                self.name,
                warn_message=f"Implicitly cleaning up {self!r}",
            )

    def __exit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None:
        if not self.keep_directory:
            super().__exit__(exc_type, exc_val, exc_tb)

    def get_path(self) -> Path:
        return Path(self.name)
