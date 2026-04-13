import tempfile
import weakref
from pathlib import Path
from types import TracebackType

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

    def __exit__(
        self,
        exc: type[BaseException] | None,
        value: BaseException | None,
        tb: TracebackType | None,
    ) -> None:
        if not self.keep_directory:
            super().__exit__(exc, value, tb)

    def get_path(self) -> Path:
        return Path(self.name)
