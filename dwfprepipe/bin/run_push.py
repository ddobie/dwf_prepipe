from dwfprepipe.push import CTIOPush
from dwfprepipe.utils import get_logger

import argparse

def parse_args():
    # Input Keyword Default Values
    dir_def = "/home4/images/fits/2019B-0253/"
    Qs_def=0.000055
    method_def='p'
    nbundle_def=4
    exp_min_def=-1
    
    
    # Parse Inputs
    parser = argparse.ArgumentParser(description='DWF_Prepipe push script for raw data from CTIO', formatter_class=argparse.RawDescriptionHelpFormatter)
    
    parser.add_argument('-d',
                        '--data_dir',
                        metavar='DIRECTORY',
                        type=str,
                        default=dir_def,
                        help='Directory where tarballs of compressed files are placed'
                        )
    
    parser.add_argument('-q',
                        '--Qs',
                        metavar='DIRECTORY',
                        type=float,
                        default=Qs_def,
                        help='Qstep for fits2jpeg compression'
                        )
    
    parser.add_argument('--method',
                        metavar='PROTOCOL',
                        type=str,
                        default=method_def,
                        help='File Transfer method:(s)erial, (p)arrallel, (b)undle, (l)ist, (e)nd of night'
                        )
    
    parser.add_argument('--nbundle',
                        metavar='NUMBER',
                        type=int,
                        default=nbundle_def,
                        help='Number of Files to bundle together'
                        )
    
    parser.add_argument('--exp_min',
                        metavar='NUMBER',
                        type=int,
                        default=exp_min_def,
                        help='Exposure Number Start for end of night file transfer catchup'
                        )
    
    parser.add_argument('--debug',
                        action="store_true",
                        help='Turn on debug output.'
                        )
    
    parser.add_argument('--quiet',
                        action="store_true",
                        help='Turn off all non-essential debug output'
                        )           

    args = parser.parse_args()
    
    return args
    
if __name__ == '__main__':
    start = datetime.datetime.now()
    
    args = parse_args()
    
    logfile = "prepipe_push_{}.log".format(
        start.strftime("%Y%m%d_%H:%M:%S")
    )
    
    logger = get_logger(args.debug, args.quiet, logfile=logfile)

    Push = CTIOPush(args.data_dir, args.Qs, args.method, args.nbundle)
    
    Push.listen()
