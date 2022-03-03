import numpy as np 
import os 

file_directory = '/fred/oz100/pipes/DWF_PIPE/CTIO_PUSH/untar/'
filenames = []
for filename in os.listdir(file_directory):
    if filename.startswith('DECam_00912854'):
        filenames.append(filename)

    sbatch_name = 'cals1.sbatch'
    sbatch_file=open(sbatch_name, 'w')
    sbatch_file.write('#!/bin/bash \n')
    sbatch_file.write('#SBATCH --output=compress.out \n')
    sbatch_file.write('#SBATCH --error=compress.err \n')
    sbatch_file.write('#SBATCH --time=00:05:00 \n')
    sbatch_file.write('#SBATCH --reservation=dwf \n')
    sbatch_file.write('source .bashrc \n')
    #sbatch_file.write('source .bash_pipe3 \n')
    
    
    for f in filenames: 
        sbatch_file.write('python dwf_prepipe_processccd.py -i ' + f + ' \n')
        sbatch_file.write('wait\n')
    sbatch_file.close() 
print(filenames)

os.system('sbatch ' + sbatch_name )

