import datetime
import os
import argparse

from dwfprepipe.push import CTIOPush
from dwfprepipe.utils import get_logger


def parse_args():
    # Input Keyword Default Values
    method_def = 'p'
    nbundle_def = 4
    exp_min_def = -1

    # Parse Inputs
    parser = argparse.ArgumentParser()

    parser.add_argument('-d',
                        '--data-dir',
                        metavar='DIRECTORY',
                        type=str,
                        default=None,
                        help='Directory where tarballs of compressed '
                             'files are placed. If not supplied, defaults to '
                             'DATA_DIR environment variable.'
                        )

    parser.add_argument('-q',
                        '--Qs',
                        type=float,
                        default=None,
                        help='Qstep for fits2jpeg compression. If not '
                             'supplied, defaults to QS environment variable.'
                        )

    parser.add_argument('--method',
                        metavar='PROTOCOL',
                        type=str,
                        default=method_def,
                        help='File Transfer method:(s)erial, (p)arallel, '
                             '(b)undle, (l)ist, (e)nd of night. Defaults to '
                             'parallel.'
                        )

    parser.add_argument('--nbundle',
                        metavar='NUMBER',
                        type=int,
                        default=nbundle_def,
                        help='Number of Files to bundle together. Defaults to '
                             '4.'
                        )

    parser.add_argument('--exp-min',
                        metavar='NUMBER',
                        type=int,
                        default=exp_min_def,
                        help='Exposure Number Start for end of night file '
                             'transfer catchup. Only relevant for method=e. '
                             'Defaults to -1, i.e. all.'
                        )

    parser.add_argument('--debug',
                        action="store_true",
                        help='Turn on debug output.'
                        )

    parser.add_argument('--quiet',
                        action="store_true",
                        help='Turn off all non-essential debug output.'
                        )

    args = parser.parse_args()

    if args.data_dir is None:
        default_data_dir = os.getenv("DATA_DIR")
        if default_data_dir is None:
            raise Exception("No data directory provided. Please set it by "
                            "passing the --data-dir argument, or by setting "
                            "The DATA_DIR environment variable."
                            )
        else:
            args.data_dir = default_data_dir

    if args.Qs is None:
        default_Qs = os.getenv("QS")
        if default_Qs is None:
            raise Exception("No compression ratio provided. Please set it by "
                            "passing the --Qs argument, or by setting "
                            "The QS environment variable."
                            )
        else:
            args.Qs = default_Qs

    return args


def main():
    """
    Run script
    """

    start = datetime.datetime.now()

    args = parse_args()

    logfile = "prepipe_push_{}.log".format(
        start.strftime("%Y%m%d_%H:%M:%S")
    )

    logger = get_logger(args.debug, args.quiet, logfile=logfile)

    logger.debug("Running with arguments:")
    for arg, value in sorted(vars(args).items()):
        logger.debug(f"{arg}: {value}")

    Push = CTIOPush(args.data_dir, args.Qs, args.method, args.nbundle)

    if Push.method == 'end of night':
        Push.process_endofnight(args.exp_min)
    else:
        Push.listen()


if __name__ == '__main__':
    main()
