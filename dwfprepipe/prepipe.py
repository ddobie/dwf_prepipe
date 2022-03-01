#!/usr/bin/env python3
#example usage ./dwf_prepipe.py /fred/oz100/fstars/DWF_Unpack_Test/push/
import os
import time
import math
import sys
import glob
import argparse
import warnings
import multiprocessing
import subprocess

import astropy.io.fits as pyfits

from typing import Union

###############################################################
###     CHANGE RESIVATION NAME HERE IF NEEDED           ######
res_name = 'dwf'
#run_date = args.run_date
#print(run_date)
###############################################################

sbatch_template_text = '''#!/bin/bash
#SBATCH -J {qroot}
#SBATCH -o {qroot_path}.stdout
#SBATCH -e {qroot_path}.stderr
#SBATCH --time={walltime}
#SBATCH -A {ozstar_proj}
#SBATCH --nodes={nodes}
#SBATCH --ntasks-per-node={ppn}
#SBATCH --mem={mem}
#SBATCH --tmp={tmp}
{res_str}

echo ------------------------------------------------------
echo Automated script by dwf_prepipe
echo ------------------------------------------------------
echo ------------------------------------------------------
echo -n 'Job is running on node '; cat $SLURM_NODELIST
echo ------------------------------------------------------
echo SBATCH: sbatch is running on $SLURM_SUBMIT_HOST
echo SBATCH: working directory is $SLURM_SUBMIT_DIR
echo SBATCH: job identifier is $SLURM_JOB_ID
echo SBATCH: job name is $SLURM_JOB_NAME

echo ------------------------------------------------------
echo Just in case, do the below:


echo $HOST

{jobs_str}

wait

echo ------------------------------------------------------
'''



class Prepipe:
    def __init__(self,
                 path_to_watch: str,
                 path_to_untar: str,
                 path_to_sbatch: str,
                 run_date,
                 res_name: Union[str, None] = None
                 ):
        """
        Constructor method.
        
        Args:
            path_to_watch: Directory to watch for new files.
            path_to_untar: Directory to untar new files into.
            path_to_sbatch:
            run_date:
            res_name: Name of the ozstar reservation, defaults to None.
        
        Returns:
            None
        """

        self.path_to_watch = path_to_watch
        self.path_to_untar = path_to_untar
        self.path_to_sbatch = path_to_sbatch
        self.run_date = run_date
        
        self.set_sbatch_vars(res_name)
        
        self.logger = logging.getLogger('dwf_prepipe.prepipe.Prepipe')
        
        
    def set_sbatch_vars(self,
                        res_name: Union[str, None] = None,
                        walltime: str = '00:05:00',
                        queue: str = 'bryan',
                        nodes: int = 1,
                        ppn: int = 16,
                        mem: str = '90G',
                        tmp: str = '4G',
                        ):
        """
        Set the variables to be used in the sbatch template
        
        Args:
            res_name: Name of the ozstar reservation, defaults to None.
            walltime: Walltime of each job, defaults to 5 minutes.
            queue: Queue to use, defaults to "bryan".
            nodes: Number of nodes to request, defaults to 1.
            ppn: 
            mem: Total memory to request, defaults to 90G.
            tmp: Temporary memory to request, defaults to 4G.
        
        Returns:
            None
        """

        self.walltime = walltime
        self.queue = queue
        self.nodes = nodes
        self.ppn = ppn
        self.mem = mem
        self.tmp = tmp
        self.res_name = res_name
        
        if res_name is None:
            self.res_str = ''
        else:
            self.res_str = '#SBATCH --reservation={}'.format(self.res_name)
    
    
    def unpack(self,
               file_name: Union[Path, str],
               ccdlist: Union[List[int], None] = None,
               n_per_ccd: int = 15
               ):
        """
        Uncompress new file + create & submit assosciated sbatch scripts
        
        Args:
            file_name: File to unpack
            ccdlist: List of CCDs to process, defaults to None.
            n_per_ccd: 
            
        Returns:
            None
        """

        if ccdlist is None:
            ccdlist = list(map(str, range(1,60)))

        DECam_Root=

        #Untar new file
        self.logger.info(f'Unpacking: {file_name}')
        
        try:
            subprocess.check_call(['tar',
                                   '-xf',
                                   push_path+file_name,
                                   '-C',
                                   untar_path
                                   ])
        except subprocess.CalledProcessError:
            self.logger.warning(f"FAILED UN-TAR {file_name}. Skipping...")
            pass
        
        Exposure=DECam_Root.split('_')[1]

        #Create Qsub scripts for new file with n_per_ccd jobs per script
        
        n_scripts=math.ceil(len(ccdlist)/n_per_ccd)
        self.logger.info(f'Writing {nscripts} sbatch scripts for {file_name}')
        
        for n in range(n_scripts):
            ccds = ccdlist[n_per_ccd*n:(n+1)*n_per_ccd]
            dwf_prepipe_sbatchccds(filename,
                                   script_num,
                                   ccds,
                                   sbatch_path,
                                   push_path,
                                   run_date
                                   )

    

        
        
    #Write Qsub Script & submit to queue
    def dwf_prepipe_sbatchccds(self, filename, script_num, ccds, sbatch_path,push_path, run_date):
        filename_root = file_name.split('.')[0]
        qroot = DECam_Root+'_q'+str(n+1)
        
        image_list=[filename_root+'_'+f+'.jp2' for f in ccds]

        sbatch_out_dir=os.path.join(sbatch_path,'out/')

        sbatch_name=sbatch_path+qroot+'.sbatch'

        self.logger.info(f"Creating Script: {sbatch_name} "
                         f"for CCDs {min(ccds)} to {max(ccds)}"
                         )
        
        jobs_str_temp = '~/dwf_prepipe/dwf_prepipe_processccd.py -i {0} -d {1} &\n'
        for image in image_list:
            jobs_str += jobs_str_temp.format(image, self.run_date)
        
        
        
        sbatch_text = sbatch_template_text.format(qroot=qroot,
                                                  qroot_path=qroot_path,
                                                  walltime=self.walltime,
                                                  nodes=self.nodes
                                                  ppn=self.ppn
                                                  mem=self.mem
                                                  tmp=self.tmp
                                                  res_str=self.res_str
                                                  )
                                                  
        
        sbatch_file = f.open(sbatch_name, 'w')
        sbatch_file.write(sbatch_text)
        sbatch_file.close()
        
        subprocess.run(['sbatch',sbatch_name])

    def run(self):
        self.logger.info("Now running!")
        self.logger.info(f"Monitoring: {self.path_to_watch}")
        
        glob_str = str(self.path_to_watch)+'*.fits.fz'
        
        before = glob.glob(glob_str)
        
        while True:
            after = glob.glob(glob_str)
            added = [f for f in after if not f in before]
            removed = [f for f in before if not f in after]

            if added:
                added_str = ", ".join(added)
                self.logger.info("Added: {added_str}")
            if removed:
                removed_str = ", ".join(removed)
                self.logger.info(f"Removed: {removed_str}")

            for f in added:
                self.unpack(f)
               
            before = after
            time.sleep(5)
        
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
