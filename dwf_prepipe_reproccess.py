#!/usr/bin/env python3
#example usage ./dwf_prepipe.py /fred/oz100/fstars/DWF_Unpack_Test/push/
import os, time
import math
import sys
import argparse
import subprocess
from numpy import loadtxt
import dwf_prepipe

def main():
	DWF_Push = "/fred/oz100/fstars/pipes/DWF_PIPE/CTIO_PUSH/"

	parser = argparse.ArgumentParser(description='Reproccess a list of files',
		formatter_class=argparse.RawDescriptionHelpFormatter)
	parser.add_argument('reprocess_list',metavar='LIST_PATH',type=str,
		help='List of files to reprocess')
	parser.add_argument('--push_dir',metavar='DIRECTORY',type=str,default=DWF_Push,
		help='Directory where tarballs of compressed files are placed')

	args = parser.parse_args()

	path_to_watch = args.push_dir
	path_to_untar = args.push_dir+'untar/'
	path_to_sbatch = args.push_dir+'sbatch/'
	with open(args.reprocess_list) as f: files=f.read().strip()
	files=files.splitlines()

	for f in files:
		if(f):
			print('Reprocessing: '+f)
			dwf_prepipe.dwf_prepipe_unpack(f,path_to_watch,path_to_untar,path_to_sbatch)
			time.sleep(60)

if __name__ == '__main__':
    main()
