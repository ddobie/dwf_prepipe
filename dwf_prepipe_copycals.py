#!/usr/bin/env python3
#Copies Calibrations for pipeloop from one directory to another.

import os
import shutil
import glob
import argparse
import subprocess

def main():
    pipe_dir='/fred/oz100/pipes/arest/DECAM/DEFAULT/rawdata/'

    parser = argparse.ArgumentParser(description='Copy Cals for armins pipeline',
        formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument('-p','--pipe_dir',metavar='DIRECTORY',type=str,default=pipe_dir,
        help='Directory where tarballs of compressed files are placed')
    parser.add_argument('-i','--input_ut',metavar='DIRECTORY',type=str,
        help='Directory where tarballs of compressed files are placed')
    parser.add_argument('-o','--dest_ut',metavar='DIRECTORY',type=str,
        help='Directory where tarballs of compressed files are placed')
    args = parser.parse_args()


    source_utdir=args.pipe_dir+args.input_ut+'/'
    dest_utdir=args.pipe_dir+args.dest_ut+'/'

    source_subdirs=[name for name in os.listdir(source_utdir)
                if os.path.isdir(os.path.join(source_utdir, name))]

    if (not os.path.isdir(dest_utdir)) and source_subdirs:
        print('Creating Directory: '+dest_utdir)
        os.makedirs(dest_utdir)

    for ccd in source_subdirs:
        #bias=glob.glob(source_utdir+ccd+'/bias.'+args.input_ut+'.*')
        #flats=glob.glob(source_utdir+ccd+'/domeflat.[a-z].'+args.input_ut+'.*')
        bias=glob.glob(source_utdir+ccd+'/bias.*')
        flats=glob.glob(source_utdir+ccd+'/domeflat.[a-z].*')


        if not os.path.isdir(dest_utdir+ccd+'/'):
            print('Creating Directory: '+dest_utdir+ccd+'/')
            os.makedirs(dest_utdir+ccd)
        for im in bias: shutil.copy(im,dest_utdir+ccd+'/')
        for im in flats: shutil.copy(im,dest_utdir+ccd+'/')

if __name__ == '__main__':
    main()
