import numpy as np 
import os 

file_directory = '/fred/oz100/pipes/DWF_PIPE/CTIO_PUSH/untar/'
for filename in os.listdir(file_directory):
    if filename.startswith('DECam_0091286'):
        os.system('python dwf_prepipe_processccd.py -i ' + filename)

