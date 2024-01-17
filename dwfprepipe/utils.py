import logging
import logging.handlers
import logging.config
import time
from pathlib import Path

from typing import Optional, Union

try:
    import colorlog
    use_colorlog = True
except ImportError:
    use_colorlog = False


def get_logger(debug: bool,
               quiet: bool,
               logfile: Optional[Union[str, Path]] = None
               ):
    """
    Initiate a logger.

    Args:
        debug: Set the logging level to debug.
        quiet: Suppress all non-essential output by setting the logging level
            to warning.
        logfile: File to write the log to.

    Returns:
        A logger.
    """

    logger = logging.getLogger()
    s = logging.StreamHandler()
    if logfile is not None:
        fh = logging.FileHandler(logfile)
        fh.setLevel(logging.DEBUG)
    logformat = '[%(asctime)s] - %(levelname)s - %(message)s'

    if use_colorlog:
        formatter = colorlog.ColoredFormatter(
            "%(log_color)s[%(asctime)s] - %(levelname)s - %(blue)s%(message)s",
            datefmt="%Y-%m-%d %H:%M:%S",
            reset=True,
            log_colors={
                'DEBUG': 'cyan',
                'INFO': 'green',
                'WARNING': 'yellow',
                'ERROR': 'red',
                'CRITICAL': 'red,bg_white', },
            secondary_log_colors={},
            style='%'
        )
    else:
        formatter = logging.Formatter(logformat, datefmt="%Y-%m-%d %H:%M:%S")

    s.setFormatter(formatter)

    if debug:
        s.setLevel(logging.DEBUG)
    else:
        if quiet:
            s.setLevel(logging.WARNING)
        else:
            s.setLevel(logging.INFO)

    logger.addHandler(s)

    if logfile is not None:
        fh.setFormatter(formatter)
        logger.addHandler(fh)

    logger.setLevel(logging.DEBUG)

    return logger


def wait_for_file(filepath: Union[str, Path],
                  wait_time: Union[int, float] = 3,
                  max_wait: Union[int, float] = 120
                  ):
    """
    Check if a file is still being written, and if so, wait for it to finish.

    Args:
        filepath: Path to the file of interest
        wait_time: Time to wait between file size checks
        max_wait: Maximum time to wait for the file to finish being written.

    Returns:
        A bool that is True if the file has been written and False otherwise.
    """
    if isinstance(filepath, str):
        filepath = Path(filepath)

    waited_time = 0
    fsize_old = filepath.stat().st_size

    while True:
        time.sleep(wait_time)
        waited_time += wait_time

        fsize_new = filepath.stat().st_size
        if fsize_new == fsize_old:
            return True
        elif waited_time > max_wait:
            return False

        fsize_old = fsize_new
