import logging
import logging.handlers
import logging.config
import time

try:
    import colorlog
    use_colorlog = True
except ImportError:
    use_colorlog = False

def get_logger(debug, quiet, logfile=None):
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
                  wait_time=0.5: Union[int, float],
                  max_wait=15: Union[int, float]
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
    if type(filepath) == str:
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
