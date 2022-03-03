import argparse
import datetime

from pathlib import Path

from dwfprepipe.prepipe import Prepipe
from dwfprepipe.utils import get_logger

def parse_args():
    # Parse Inputs
    parser = argparse.ArgumentParser()
    
    parser.add_argument('--debug',
                        action="store_true",
                        help='Turn on debug output.'
                        )
    
    parser.add_argument('--quiet',
                        action="store_true",
                        help='Turn off all non-essential debug output'
                        )

    parser.add_argument('--push_dir',
                        metavar='DIRECTORY',
                        type=str,
                        default="/fred/oz100/pipes/DWF_PIPE/CTIO_PUSH/",
                        help='Path to tarball directory')

    parser.add_argument('--run_date',
                        type=str,
                        help='Date of the run night and data being unpacked'
                        )

    parser.add_argument('--res_name',
                        type=str,
                        help='Ozstar reservation name'
                        )

    args = parser.parse_args()
    
    return args
    
if __name__ == '__main__':
    start = datetime.datetime.now()
    
    args = parse_args()
    
    logfile = "prepipe_{}.log".format(
        start.strftime("%Y%m%d_%H:%M:%S")
    )
    
    logger = get_logger(args.debug, args.quiet, logfile=logfile)

    path_to_watch = Path(args.push_dir)
    path_to_untar = path_to_watch / 'untar'
    path_to_sbatch = path_to_watch / 'sbatch'
    
    prepipe = Prepipe(path_to_watch,
                      path_to_untar,
                      path_to_sbatch,
                      args.run_date,
                      args.res_name
                      )
    
    #prepipe.listen()
