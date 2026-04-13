import logging
import subprocess
import xml.etree.ElementTree as ET
from typing import Any
from xml.etree.ElementTree import Element

logger = logging.getLogger(__name__)


def get_qstat_job_xml(
    job_id: str | int,
) -> list[dict[str, Any]]:
    """Get detailed information for a specific job using qstat -j -xml.

    This returns comprehensive information about a single job, including
    resource requests, submission details, and task status in XML format,
    which is then parsed into a Python dictionary.

    Args:
        job_id: The job ID to query.

    Returns:
        Tuple containing:
            - List of dictionaries with detailed job information (typically one job)
            - List of error messages (if any)

    Examples:
        >>> # Get detailed info for a specific job
        >>> jobs, errors = get_qstat_job_xml(12345)
        >>> if jobs:
        ...     job = jobs[0]
        ...     print(job["JB_job_name"])
        ...     print(job["JB_owner"])
    """

    cmd = f"qstat -j {job_id} -nenv -xml"

    logger.debug(f"Executing: {cmd}")

    process = subprocess.run(
        cmd,
        encoding="utf-8",
        capture_output=True,
        shell=True,
    )

    stdout = process.stdout
    stderr = process.stderr

    if stderr:
        logger.warning(f"qstat stderr: {stderr}")

    # Parse the XML
    try:
        jobs = parse_jobinfo_xml(stdout)
        return jobs
    except ET.ParseError as e:
        logger.error(f"Failed to parse XML output: {e}")
        return []


def parse_jobinfo_xml(
    stdout_xml: str,
) -> list[
    dict[
        str,
        str
        | list[dict[str, list[dict[str, str]] | str]]
        | list[dict[str, str]]
        | dict[str, dict[str, str]]
        | dict[str, str],
    ]
]:
    """
    Parse qstat -j -xml output into a list of job info dictionaries.

    Parses the XML output from qstat -j -nenv -xml and extracts job information
    from the djob_info elements.

    Args:
        stdout_xml: Raw XML string from qstat -j -xml command.

    Returns:
        List of dictionaries, each containing detailed job information.
        Each dictionary corresponds to one element in djob_info.

    Example:
        >>> xml_output = subprocess.run("qstat -j 12345 -nenv -xml", ...).stdout
        >>> jobs = parse_jobinfo_xml(xml_output)
        >>> print(jobs[0]["JB_job_number"])
        >>> print(jobs[0]["JB_job_name"])

    Notes:
     - There are no XML attributes on UGE output
     - All <element> tags are translated to pure lists

    """

    root = ET.fromstring(stdout_xml)

    jobs: list[dict[str, Any]] = []

    # Find all djob_info/element nodes
    for element in root.findall(".//djob_info/element"):
        d = parse_element(element)

        if not isinstance(d, dict):
            raise RuntimeError(f"Expected dict from parse_element, got {type(d)}")

        jobs.append(d)
        print(d)

    return jobs


def parse_element(elem: Element) -> Any:
    """
    return string, dict or list
    """

    has_children = len(elem) > 0
    text = (elem.text or "").strip()

    if not has_children:
        return text

    child_tags = [child.tag for child in elem]
    if "element" in child_tags:
        return element_to_list(elem)

    return element_to_dict(elem)


def element_to_dict(
    elem: Element,
) -> dict[
    str,
    str
    | list[dict[str, list[dict[str, str]] | str]]
    | list[dict[str, str]]
    | dict[str, dict[str, str]]
    | dict[str, str],
]:
    children = list(elem)
    child_map: dict[str, list[Any]] = {}
    for child in children:
        child_val = parse_element(child)
        child_map.setdefault(child.tag, []).append(child_val)

    d: dict[str, Any] = {}
    for tag, items in child_map.items():
        d[tag] = items[0] if len(items) == 1 else items

    return d


def element_to_list(elem: Element) -> list[dict[str, list[dict[str, str]] | str]]:
    out: list[Any] = []
    for child in elem:
        if child.tag != "element":
            continue
        val = parse_element(child)
        if isinstance(val, list):
            out.extend(val)
        else:
            out.append(val)

    return out
