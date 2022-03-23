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
                  wait_time: Union[int, float] = 0.5,
                  max_wait: Union[int, float] = 15
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


def listen(logger,
           path_to_watch: Union[str, Path],
           function_to_run,
           extension: str = ".fits.fz",
           sleep_time: float = 1.0
           ):
    """
    Listen for new images, push or package them.

    Args:
        logger: Where stuff will be logged.
        path_to_watch: Path to listen in on.
        function_to_run: Function to run when a file (or list of files)
                         is found.
        extension: Extension to listen for.
        sleep_time: Seconds to sleep after a loop without finding a new file.

    Returns:
        None
    """
    if isinstance(path_to_watch, str):
        path_to_watch = Path(path_to_watch)

    logger.info(f"Monitoring: {str(path_to_watch)}")

    logger.debug(f"Checking files with extension: {extension}")
    before = list(path_to_watch.glob("*" + extension))
    existingfiles_str = ', '.join([str(f) for f in before])
    logger.debug(f"Existing files: {existingfiles_str}")

    while True:
        after = list(path_to_watch.glob("*" + extension))
        after_str = ', '.join([str(f) for f in after])
        logger.debug(f"Current files: {after_str}")
        added = [f for f in after if f not in before]
        removed = [f for f in before if f not in after]

        if added:
            added_str = ', '.join([str(f) for f in added])
            logger.info(f"Added: {added_str}")

            function_to_run(added)

        if removed:
            removed_str = ', '.join([str(f) for f in removed])
            logger.info(f"Removed: {removed_str}")

        before = after

        if not added:
            time.sleep(sleep_time)
