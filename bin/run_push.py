from dwfprepipe.push import CTIOPush
from dwfprepipe.utils import get_logger
import datetime

import argparse

def parse_args():
    # Input Keyword Default Values
    method_def='p'
    nbundle_def=4
    exp_min_def=-1
    
    
    # Parse Inputs
    parser = argparse.ArgumentParser()
    
    parser.add_argument('-d',
                        '--data-dir',
                        metavar='DIRECTORY',
                        type=str,
                        default=None,
                        help='Directory where tarballs of compressed '
                             'files are placed.'
                        )
    
    parser.add_argument('-q',
                        '--Qs',
                        type=float,
                        default=None,
                        help='Qstep for fits2jpeg compression.'
                        )
    
    parser.add_argument('--method',
                        metavar='PROTOCOL',
                        type=str,
                        default=method_def,
                        help='File Transfer method:(s)erial, (p)arallel, '
                             '(b)undle, (l)ist, (e)nd of night.'
                        )
    
    parser.add_argument('--nbundle',
                        metavar='NUMBER',
                        type=int,
                        default=nbundle_def,
                        help='Number of Files to bundle together'
                        )
    
    parser.add_argument('--exp-min',
                        metavar='NUMBER',
                        type=int,
                        default=exp_min_def,
                        help='Exposure Number Start for end of night file '
                             'transfer catchup.'
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
        default_data_dir = os.getenviron("DATA_DIR")
        if default_data_dir is None:
            raise Exception("No data directory provided. Please set it by "
                            "passing the --data-dir argument, or by setting "
                            "The DATA_DIR environment variable."
                            )
        else:
            args.data_dir = default_data_dir
    
    if args.Qs is None:
        default_Qs = os.getenviron("QS")
        if default_Qs is None:
            raise Exception("No compression ratio provided. Please set it by "
                            "passing the --Qs argument, or by setting "
                            "The QS environment variable."
                            )
        else:
            args.Qs = default_Qs
    
    return args
    

if __name__ == '__main__':
    start = datetime.datetime.now()
    
    args = parse_args()
    
    logfile = "prepipe_push_{}.log".format(
        start.strftime("%Y%m%d_%H:%M:%S")
    )
    
    logger = get_logger(args.debug, args.quiet, logfile=logfile)
    
    Push = CTIOPush(args.data_dir, args.Qs, args.method, args.nbundle)
    exit()
    if Push.method == 'end of night':
        Push.process_endofnight(args.exp_min)
    else:
        Push.listen()
