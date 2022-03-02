#!/usr/bin/env python3
import os
import re
import time
import math
import sys
import glob
import argparse
import warnings
import multiprocessing
import subprocess
import importlib.resources

import astropy.io.fits as pyfits

from typing import Union



class Prepipe:
    def __init__(self,
                 path_to_watch: Union[str, Path],
                 path_to_untar: Union[str, Path],
                 path_to_sbatch: Union[str, Path],
                 run_date: str,
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
        regexp_pattern = r"^(ut[0-9][0-9][0-1][0-9][0-3][0-9])$"
        if not bool(re.match(regexp_pattern, run_date)):
            raise Exception("Run date must be in the form utYYMMDD")

        self.path_to_watch = path_to_watch
        self.path_to_untar = path_to_untar
        self.path_to_sbatch = path_to_sbatch
        self.run_date = run_date
        self.sbatch_out_dir = self.path_to_sbatch / 'out'
        
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

        file_name = Path(file_name)
        

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
        
        for script_num in range(n_scripts):
            ccds = ccdlist[n_per_ccd*script_num:(script_num+1)*n_per_ccd]
            dwf_prepipe_sbatchccds(filename, script_num, ccds)

    def _write_sbatch(self,
                      sbatch_name: Union[str, Path],
                      qroot: str,
                      jobs_str: str
                      ):
        """
        Write the Qsub script
        
        Args:
            sbatch_name: Path to write the sbatch file to.
            qroot: Qsub root name.
            jobs_str: String containing the jobs to run, one per line.
        
        Returns:
            None
        """
        
        qroot_path = self.sbatch_out_dir / qroot
        
        with importlib.resources.path(
            "dwfprepipe.data", "sbatch_template.txt"
        ) as sbatch_template_file: 
            f = open(sbatch_template_file, "r")
            sbatch_templ = f.read()
        sbatch_text = sbatch_templ.format(qroot=qroot,
                                          qroot_path=qroot_path,
                                          walltime=self.walltime,
                                          nodes=self.nodes
                                          ppn=self.ppn
                                          mem=self.mem
                                          tmp=self.tmp
                                          res_str=self.res_str
                                          jobs_str=jobs_str
                                          )
                                          
        sbatch_file = f.open(sbatch_name, 'w')
        sbatch_file.write(sbatch_text)
        sbatch_file.close()
        
        
    
    def sbatchccds(self,
                   file_name: Path,
                   script_num: int,
                   ccds: List[int]
                   ):
        """
        Write Qsub Script & submit to queue
        
        Args:
            file_name: Path to file to be processed
            script_num: Number identifying which script this is.
            ccds: CCDs to be processed in this sbatch file
        """
        
        DECam_root = file_name.stem
        qroot = f'{DECam_root}_q{script_num+1}'
        
        image_list=[f'{DECam_root}_{f}.jp2' for f in ccds]

        sbatch_name=sbatch_path / f'{qroot}.sbatch'

        self.logger.info(f"Creating Script: {sbatch_name} "
                         f"for CCDs {min(ccds)} to {max(ccds)}"
                         )
        
        jobs_str_temp = '~/dwf_prepipe/dwf_prepipe_processccd.py -i {0} -d {1} &\n'
        jobs_str = ''
        for image in image_list:
            jobs_str += jobs_str_temp.format(image, self.run_date)
        
        self._write_sbatch(sbatch_name, qroot, jobs_str)
        
        if sbatch_name.is_file():
            subprocess.run(['sbatch', str(sbatch_name]))
        else:
            logger.critical(f"{sbatch_name} does not exist!")
                                                  
        
        

    def listen(self):
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
