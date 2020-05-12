#!/usr/bin/env python3
#example usage ./dwf_prepipe.py /fred/oz100/fstars/DWF_Unpack_Test/push/
import os, time
import math
import sys
import glob
import argparse
import warnings
import multiprocessing
import subprocess
import astropy.io.fits as pyfits

###############################################################
###     CHANGE RESIVATION NAME HERE IF NEEDED           ######
res_name = 'dwf'
#run_date = args.run_date
#print(run_date)
###############################################################


#Uncompress new file + create & submit assosciated sbatch scripts
def dwf_prepipe_unpack(file_name,push_path,untar_path,sbatch_path,run_date):
	ccdlist=['1', '2', '3', '4', '5', '6', '7', '8', '9', '10', '11', '12', '13', '14', '15', '16', '17', '18', '19', '20', '21', '22', '23', '24', '25', '26', '27', '28', '29', '30', '31', '32', '33', '34', '35', '36', '37', '38', '39', '40', '41', '42', '43', '44', '45', '46', '47', '48', '49', '50', '51', '52', '53', '54', '55', '56', '57', '58', '59']
	#Truncated CCD list for testing
	#ccdlist=['2', '3', '4', '5', '6', '7', '8', '9', '10', '11', '12', '13', '14', '15', '16'] #truncated version for test

	DECam_Root=file_name.split('.')[0]

	#Untar new file
	print('Unpacking:\t'+file_name)
	try:
		subprocess.check_call(['tar','-xf',push_path+file_name,'-C',untar_path])
	except subprocess.CalledProcessError:
		print(f"FAILED UN-TAR {file_name}. Skipping...")
		pass
	Exposure=DECam_Root.split('_')[1]

	#Create Qsub scripts for new file with n_per_ccd jobs per script
	n_per_ccd=15
	n_scripts=math.ceil(len(ccdlist)/n_per_ccd)
	print('Writing '+str(n_scripts)+' sbatch scripts for '+file_name)
	for n in range(n_scripts):
		dwf_prepipe_sbatchccds(DECam_Root,DECam_Root+'_q'+str(n+1),ccdlist[n_per_ccd*n:(n+1)*n_per_ccd],sbatch_path,push_path, run_date)

#Write Qsub Script & submit to queue
def dwf_prepipe_sbatchccds(filename_root,qroot,ccds,sbatch_path,push_path, run_date):
	image_list=[filename_root+'_'+f+'.jp2' for f in ccds]

	sbatch_out_dir=sbatch_path+'out/'

	sbatch_name=sbatch_path+qroot+'.sbatch'

	print('Creating Script: '+sbatch_name+' for CCDs '+min(ccds)+' to '+max(ccds))
	sbatch_file=open(sbatch_name,'w')

	walltime='00:05:00'
	queue='bryan'
	nodes='1'
	ppn='16'
	mem='90G'
	tmp='4G'
	res=res_name

	sbatch_file.write('#!/bin/bash \n')
	sbatch_file.write('#SBATCH -J {0}\n'.format(qroot))
	sbatch_file.write('#SBATCH -o {0}{1}.stdout\n'.format(sbatch_path+'out/',qroot))
	sbatch_file.write('#SBATCH -e {0}{1}.stderr\n'.format(sbatch_path+'out/',qroot))
	sbatch_file.write('#SBATCH --time={0}\n'.format(walltime))
	# sbatch_file.write('#SBATCH -p {0}\n'.format(queue))
	sbatch_file.write('#SBATCH -A oz100\n')
	sbatch_file.write('#SBATCH --nodes={0}\n'.format(nodes))
	sbatch_file.write('#SBATCH --ntasks-per-node={0}\n'.format(ppn))
	sbatch_file.write('#SBATCH --mem={0}\n'.format(mem))
	sbatch_file.write('#SBATCH --tmp={0}\n'.format(tmp))
	sbatch_file.write('#SBATCH --reservation={0}\n'.format(res))

	sbatch_file.write('echo ------------------------------------------------------\n')
	sbatch_file.write('echo Automated script by dwf_prepipe\n')
	sbatch_file.write('echo ------------------------------------------------------\n')
	sbatch_file.write('echo ------------------------------------------------------\n')
	sbatch_file.write("echo -n 'Job is running on node '; cat $SLURM_NODELIST\n")
	sbatch_file.write('echo ------------------------------------------------------\n')
	sbatch_file.write('echo SBATCH: sbatch is running on $SLURM_SUBMIT_HOST\n')
	sbatch_file.write('echo SBATCH: working directory is $SLURM_SUBMIT_DIR\n')
	sbatch_file.write('echo SBATCH: job identifier is $SLURM_JOB_ID\n')
	sbatch_file.write('echo SBATCH: job name is $SLURM_JOB_NAME\n')

	sbatch_file.write('echo ------------------------------------------------------\n')
	sbatch_file.write('echo Just in case, do the below:\n')

	
	sbatch_file.write('echo $HOST\n')

	for f in image_list:
		#sbatch_file.write('/fred/oz100/dvohl/dwf_prepipe/dwf_prepipe_processccd.py -i {0} &\n'.format(f))
		sbatch_file.write('~/dwf_prepipe/dwf_prepipe_processccd.py -i {0} -d {1} &\n'.format(f, run_date))
	#desperate test
	#sbatch_file.write('source ~/.bash_ozstarpython\n')
	sbatch_file.write('wait\n')

	sbatch_file.write('echo ------------------------------------------------------\n')
	#sbatch_file.write('echo Safety Cleanup for the local disk:\n')

	#sbatch_file.write('rm $JOBFS/dwf/*.jp2\n')
	#sbatch_file.write('rm $JOBFS/dwf/*.fits\n')
	sbatch_file.write('echo ------------------------------------------------------\n')

	sbatch_file.close()
	subprocess.run(['sbatch',sbatch_name])

def main():
    DWF_Push = "/fred/oz100/pipes/DWF_PIPE/CTIO_PUSH/"
    parser = argparse.ArgumentParser(description='Handle File Ingests for the DWF pipeline', formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument('--push_dir',metavar='DIRECTORY',type=str,default=DWF_Push, help='Directory where tarballs of compressed files are placed')
    parser.add_argument('--run_date', type=str, help='Date of the run night and data being unpacked')
    #args = parser.parse_args()
    args = parser.parse_args()
    path_to_watch = args.push_dir
    path_to_untar = args.push_dir+'untar/'
    path_to_sbatch = args.push_dir+'sbatch/'
    before = dict ([(f, None) for f in os.listdir (path_to_watch)])
    run_date =args.run_date
	#dwf_prepipe_unpack('DECam_00504110.tar',path_to_watch,path_to_untar,path_to_sbatch)
    print('Monitoring Directory:'+path_to_watch)
    while 1:
        after = dict ([(f, None) for f in os.listdir (path_to_watch)])
        added = [f for f in after if not f in before]
        removed = [f for f in before if not f in after]

        if added: print("Added: ", ", ".join (added))
        if removed: print("Removed: ", ", ".join (removed))

        for f in added:
            dwf_prepipe_unpack(f,path_to_watch,path_to_untar,path_to_sbatch, run_date)

        before = after
        time.sleep (5)


if __name__ == '__main__':
    main()
