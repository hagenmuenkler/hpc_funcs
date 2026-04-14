import logging
import os
import shutil
import subprocess
import time
from collections.abc import Iterator
from pathlib import Path
from typing import Optional

logger = logging.getLogger(__name__)


def which(cmd: Path | str) -> Path | None:
    """Check if command exists in environment"""
    path_ = shutil.which(cmd)

    if path_ is None:
        return None

    path = Path(path_)

    return path


def switch_workdir(path: Path | None) -> bool:
    """Check if it makes sense to change directory"""

    if path is None:
        return False

    if path == "":
        return False

    if path == "./":
        return False

    if path == ".":
        return False

    if not os.path.exists(path):
        raise ValueError(f"Cannot change directory, does not exist: {path}")

    return True


class StreamResult:
    """Result of a streaming command execution.

    Iterating over this object yields stdout lines. After iteration is complete,
    stderr is available via the `.stderr` property.

    Example:
        result = stream("my_command")
        for line in result:
            print(line)
        if result.stderr:
            print(f"Errors: {result.stderr}")
    """

    def __init__(self, process: subprocess.Popen) -> None:
        self._process = process
        self._stderr: str | None = None
        self._exhausted = False

    def __iter__(self) -> Iterator[str]:
        if self._process.stdout is None:
            return

        yield from iter(self._process.stdout.readline, "")

        # Capture stderr after stdout is exhausted
        if self._process.stderr is not None:
            self._stderr = self._process.stderr.read()

        self._process.stdout.close()
        self._exhausted = True

    @property
    def stderr(self) -> str:
        """Return stderr output. Must iterate through stdout first."""
        if not self._exhausted:
            # Consume remaining stdout if not already done
            for _ in self:
                pass
        return self._stderr or ""

    def wait(self) -> int:
        """Wait for the process to complete and return the exit code."""
        return self._process.wait()

    def close(self) -> None:
        """Close the process streams and terminate if still running."""
        if self._process.stdout:
            self._process.stdout.close()
        if self._process.stderr:
            self._process.stderr.close()
        self._process.terminate()


def stream(cmd: str, cwd: Path | None = None, shell: bool = True) -> StreamResult:
    """Execute command in directory, and stream stdout.

    Returns a StreamResult object that can be iterated to get stdout lines.
    After iteration, stderr is available via the `.stderr` property.

    :param cmd: The shell command
    :param cwd: Change directory to work directory
    :param shell: Use shell or not in subprocess
    :returns: StreamResult object for streaming stdout and accessing stderr.

    Example:
        result = stream("ls -la")
        for line in result:
            print(line, end="")
        if result.stderr:
            print(f"Errors: {result.stderr}")
    """

    if not switch_workdir(cwd):
        cwd = None

    popen = subprocess.Popen(
        cmd,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        universal_newlines=True,
        shell=shell,
        cwd=cwd,
    )

    return StreamResult(popen)


def execute(
    cmd: str,
    cwd: Path | None = None,
    shell: bool = True,
    timeout: None = None,
    check: bool = True,
) -> tuple[str, str]:
    """Execute command in directory, and return stdout and stderr

    :param cmd: The shell command
    :param cwd: Change directory to work directory
    :param shell: Use shell or not in subprocess
    :param timeout: Stop the process at timeout (seconds)
    :returns: stdout and stderr as string
    :raises: subprocess.CalledProcessError if check is True and command fails
    :raises: subprocess.TimeoutExpired if timeout is reached
    :raises: FileNotFoundError if check is True and command is not found
    """

    if not switch_workdir(cwd):
        cwd = None

    try:
        process = subprocess.run(
            cmd,
            cwd=cwd,
            encoding="utf-8",
            shell=shell,
            check=check,
            capture_output=True,
            timeout=timeout,
        )
    except subprocess.CalledProcessError as exc:
        logger.error("Command %s failed", cmd)
        logger.error("stdout: %s", exc.stdout)
        logger.error("stderr: %s", exc.stderr)
        logger.error("returncode: %s", exc.returncode)
        raise exc

    except FileNotFoundError as exc:
        logger.error("Command %s not found:", cmd)
        if check:
            raise exc
        else:
            return "", ""

    except subprocess.TimeoutExpired as exc:
        logger.error("Command %s timed out:", cmd)
        if check:
            raise exc
        else:
            stderr = "" if exc.stderr is None else exc.stderr.decode("utf-8")
            stdout = "" if exc.stdout is None else exc.stdout.decode("utf-8")

            return stdout, stderr

    return process.stdout, process.stderr


def execute_with_retry(
    cmd: str,
    cwd: Path | None = None,
    shell: bool = True,
    timeout: None = None,
    max_retries: int = 3,
    update_interval: int = 5,
) -> tuple[str, str]:
    """Execute command in directory, and return stdout and stderr

    :param cmd: The shell command
    :param cwd: Change directory to work directory
    :param shell: Use shell or not in subprocess
    :param timeout: Stop the process at timeout (seconds)
    :param max_retries: How many times to rerun the command before raising an error
    :param update_interval: How long to wait between retries
    :returns: stdout and stderr as string
    :raises: subprocess.CalledProcessError if command fails more than max_retries
    :raises: subprocess.TimeoutExpired if timeout is reached and the command failed more
        than max_retries
    :raises: FileNotFoundError if command is not found
    """

    num_retries = 0
    while True:
        try:
            stdout, stderr = execute(cmd, cwd=cwd, shell=shell, timeout=timeout, check=True)
            return stdout, stderr
        except (subprocess.TimeoutExpired, subprocess.CalledProcessError) as exc:
            logger.warning("Error while executing %s. Try again later.", cmd)
            time.sleep(update_interval)
            if num_retries >= max_retries:
                logger.error("Max retries reached for command %s", cmd)
                raise exc
            num_retries += 1
