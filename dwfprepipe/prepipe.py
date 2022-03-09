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
import logging

import astropy.io.fits as pyfits

from pathlib import Path
from typing import Union, List
from utils import wait_for_file


class PrepipeInitError(Exception):
    """
    A defined error for a problem encountered in the Prepipe initialisation
    """
    pass


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
        
        Raises:
            PrepipeInitError: Problems found in the requested settings
        """
        
        self.logger = logging.getLogger('dwf_prepipe.prepipe.Prepipe')
        
        self.path_to_watch = Path(path_to_watch)
        self.path_to_untar = Path(path_to_untar)
        self.path_to_sbatch = Path(path_to_sbatch)
        self.run_date = run_date
        self.sbatch_out_dir = self.path_to_sbatch / 'out'
        
        self.set_sbatch_vars(res_name)
        
        valid_settings = self._validate_settings()
        if not valid_settings:
            raise PrepipeInitError("Problems found in the requested settings! "
                                   "Please address and try again."
                                   )
        self.logger.info("Successfully initialised Prepipe.")
        self.logger.info(f"Running with path_to_watch={self.path_to_watch}")
        self.logger.info(f"Running with path_to_untar={self.path_to_untar}")
        self.logger.info(f"Running with path_to_sbatch={self.path_to_sbatch}")
        self.logger.info(f"Running with run_date={self.run_date}")
        
        
    def _validate_settings(self):
        """
        Validate the requested settings
        
        Args:
            None
        
        Returns:
            Bool
        """
        
        regexp_pattern = r"^(ut[0-9][0-9][0-1][0-9][0-3][0-9])$"
        if not bool(re.match(regexp_pattern, self.run_date)):
            self.logger.critical("Run date must be in the form utYYMMDD")
            return False
        
        if not self.path_to_watch.is_dir():
            self.logger.critical(f"The provided path to watch, "
                                 f"{self.path_to_watch}, does not exist!"
                                 )
            return False
        
        if not self.path_to_untar.is_dir():
            self.logger.critical(f"The provided path to untar, "
                                 f"{self.path_to_untar}, does not exist!"
                                 )
            return False
        
        if not self.path_to_sbatch.is_dir():
            self.logger.critical(f"The provided path to sbatch, "
                                 f"{self.path_to_sbatch}, does not exist!"
                                 )
            return False
        
        if not self.path_to_sbatch.is_dir():
            self.logger.critical(f"The provided sbatch output directory, "
                                 f"{self.sbatch_out_dir}, does not exist!"
                                 )
            return False
        
        return True
        
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

        self.logger.info(f"Setting walltime to {walltime}")
        self.walltime = walltime
        self.logger.info(f"Setting queue to {queue}")
        self.queue = queue
        self.logger.info(f"Setting nodes to {nodes}")
        self.nodes = nodes
        self.logger.info(f"Setting ppn to {ppn}")
        self.ppn = ppn
        self.logger.info(f"Setting mem to {mem}")
        self.mem = mem
        self.logger.info(f"Setting tmp to {tmp}")
        self.tmp = tmp
        self.res_name = res_name
        
        if res_name is None:
            self.res_str = ''
            self.logger.warning("Warning: not using a reservation")
        else:
            self.res_str = '#SBATCH --reservation={}'.format(self.res_name)
            self.logger.info(f"Setting walltime to {walltime}")
    
    
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
                                          nodes=self.nodes,
                                          ppn=self.ppn,
                                          mem=self.mem,
                                          tmp=self.tmp,
                                          res_str=self.res_str,
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
        with importlib.resources.path(
            "dwfprepipe.bin", "prepipe_processccd.py"
        ) as process_ccd_script:
            jobs_str_temp = f'{process_ccd_script} -i {{0}} -d {{1}} &\n'
        jobs_str = ''
        for image in image_list:
            jobs_str += jobs_str_temp.format(image, self.run_date)
        
        self._write_sbatch(sbatch_name, qroot, jobs_str)
        
        if sbatch_name.is_file():
            subprocess.run(['sbatch', str(sbatch_name)])
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
                if not wait_for_file(f):
                    self.logger.info(f'{f} not written in time! Skipping...')
                    continue

                self.unpack(f)
               
            if not added:
                time.sleep(5)
            
            before = after
