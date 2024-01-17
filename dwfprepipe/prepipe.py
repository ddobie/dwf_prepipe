import re
import time
import math
import subprocess
import importlib.resources
import logging

from pathlib import Path
from typing import Union, List, Optional
from astropy.io import fits

from dwfprepipe.utils import wait_for_file

from timeit import default_timer as timer

class PrepipeInitError(Exception):
    """
    A defined error for a problem encountered in the Prepipe initialisation.
    """
    pass


class Prepipe:
    def __init__(self,
                 path_to_watch: Union[str, Path],
                 path_to_untar: Union[str, Path],
                 path_to_sbatch: Union[str, Path],
                 run_date: str,
                 res_name: Optional[str] = None,
                 dry_run: bool = False,
                 compress: str = True,
                 ):
        """
        Constructor method.

        Args:
            path_to_watch: Directory to watch for new files.
            path_to_untar: Directory to untar new files into.
            path_to_sbatch: Directory to write sbatch files to.
            run_date: UT date of the run in the form `utYYMMDD`.
            res_name: Name of the ozstar reservation, defaults to None.
            dry_run: If `True`, writes sbatch files but does not submit them.

        Returns:
            None

        Raises:
            PrepipeInitError: Problems found in the requested settings.
        """

        self.logger = logging.getLogger('dwf_prepipe.prepipe.Prepipe')

        self.path_to_watch = Path(path_to_watch)
        self.path_to_untar = Path(path_to_untar)
        self.path_to_sbatch = Path(path_to_sbatch)
        self.run_date = run_date
        self.dry_run = dry_run
        self.compress = compress
        self.sbatch_out_dir = self.path_to_sbatch / 'out'

        self.set_sbatch_vars(res_name)

        valid_settings = self._validate_settings()
        if not valid_settings:
            raise PrepipeInitError("Problems found in the requested settings! "
                                   "Please address and try again."
                                   )
        self.logger.info("Successfully initialised Prepipe.")
        self.logger.info(f"Watching {self.path_to_watch}...")
        self.logger.debug(f"Running with path_to_untar={self.path_to_untar}")
        self.logger.debug(f"Running with path_to_sbatch={self.path_to_sbatch}")
        self.logger.debug(f"Running with run_date={self.run_date}")

    def _validate_settings(self):
        """
        Validate the requested settings.

        Args:
            None

        Returns:
            bool
        """

        valid = True

        regexp_pattern = r"^(ut[0-9][0-9][0-1][0-9][0-3][0-9])$"
        if not bool(re.match(regexp_pattern, self.run_date)):
            self.logger.critical("Run date must be in the form utYYMMDD")
            valid = False

        if not self.path_to_watch.is_dir():
            self.logger.critical(f"The provided path to watch, "
                                 f"{self.path_to_watch}, does not exist!"
                                 )
            valid = False

        if not self.path_to_untar.is_dir():
            self.logger.critical(f"The provided path to untar, "
                                 f"{self.path_to_untar}, does not exist!"
                                 )
            valid = False

        if not self.path_to_sbatch.is_dir():
            self.logger.critical(f"The provided path to sbatch, "
                                 f"{self.path_to_sbatch}, does not exist!"
                                 )
            return False

        if not self.path_to_sbatch.is_dir():
            self.logger.critical(f"The provided sbatch output directory, "
                                 f"{self.sbatch_out_dir}, does not exist!"
                                 )
            valid = False

        return valid

    def set_sbatch_vars(self,
                        res_name: Union[str, None] = None,
                        walltime: str = '00:05:00',
                        queue: str = 'bryan',
                        nodes: int = 1,
                        ppn: int = 16,
                        mem: str = '90G',
                        tmp: str = '4G',
                        ozstar_proj: str = 'oz100'
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
            ozstar_proj: ozstar project code.

        Returns:
            None
        """

        self.logger.debug(f"Setting walltime to {walltime}")
        self.walltime = walltime
        self.logger.debug(f"Setting queue to {queue}")
        self.queue = queue
        self.logger.debug(f"Setting nodes to {nodes}")
        self.nodes = nodes
        self.logger.debug(f"Setting ppn to {ppn}")
        self.ppn = ppn
        self.logger.debug(f"Setting mem to {mem}")
        self.mem = mem
        self.logger.debug(f"Setting tmp to {tmp}")
        self.tmp = tmp
        self.logger.debug(f"Setting ozstar_proj to {ozstar_proj}")
        self.ozstar_proj = ozstar_proj
        self.res_name = res_name

        if res_name is None:
            self.res_str = ''
            self.logger.warning("NO RESERVATION SPECIFIED. RUNNING WITHOUT.")
        else:
            self.res_str = '#SBATCH --reservation={}'.format(self.res_name)
            self.logger.debug(f"Setting res_name to {res_name}")

    def process_file(self,
                     file_name: Union[Path, str],
                     ccdlist: Union[List[int], None] = None,
                     n_per_ccd: int = 5,
                     bad_ccds: Union[List[str], None] = ['33']
                     ):
        """
        Run the complete processing on a single file

        Args:
            file_name: File to unpack
            ccdlist: List of CCDs to process, defaults to None.
            n_per_ccd:
            bad_ccds: list of ccds to ignore.

        Returns:
            None
        """

        self.logger.info(f"Processing {file_name}...")
        if ccdlist is None:
            ccdlist = list(map(str, range(1, 60)))
            for bad_ccd in bad_ccds:
                ccdlist.remove(bad_ccd)

        file_name = Path(file_name)

        unpacked = self.unpack(file_name)
        if not unpacked:
            return

        # Create Qsub scripts for new file with n_per_ccd jobs per script
        n_scripts = math.ceil(len(ccdlist) / n_per_ccd)
        self.logger.info(f'Writing {n_scripts} sbatch scripts for {file_name}')

        for script_num in range(n_scripts):
            ccds = ccdlist[n_per_ccd * script_num:(script_num + 1) * n_per_ccd]
            self.sbatchccds(file_name, script_num, ccds)

    def split_ccds(self,
                   filepath_in: Union[Path, str],
                   output_dir: Union[Path, str],
                   ):
        """
        Split fits file into individual CCDs
        """
        self.logger.info(f"Splitting {filepath_in} into CCDs")
        output_dir = Path(output_dir)
        
        hdul = fits.open(filepath_in)
        
        primary_header = hdul[0].header

        for ext in range(len(hdul)):
            if ext == 0:
                continue
            file_name = filepath_in.name
            new_file_name = file_name.replace('.fits.fz',f'_{ext}.fits')
            
            hdu = hdul[ext]
            
            if hdu.data.shape[0] != 4146:
                continue
            
            new_header = hdu.header
            for key in primary_header:
                if key not in new_header:
                    if str(key).startswith('COMMENT') or str(key) == 'HISTORY':
                        continue
                    new_header[key] = primary_header[key]

            new_hdu = fits.PrimaryHDU(data=hdu.data, header=new_header)
            new_hdu.writeto(output_dir / new_file_name, overwrite=True)
            self.logger.debug(f"Wrote {output_dir / new_file_name}")
        hdul.close()
        
    def unpack(self,
               file_name: Union[Path, str]
               ):
        """
        Uncompress new file + create & submit assosciated sbatch scripts

        Args:
            file_name: File to unpack

        Returns:
            bool
        """

        self.logger.info(f'Unpacking: {file_name}')
        file_name = file_name.name
        try:
            subprocess_call = ['tar',
                               '-xf',
                               str(self.path_to_watch / file_name),
                               '-C',
                               str(self.path_to_untar)
                               ]
            self.logger.debug(f"Running {' '.join(subprocess_call)}")
            subprocess.check_call(subprocess_call)

        except subprocess.CalledProcessError:
            self.logger.critical(f"FAILED UN-TAR {file_name}. Skipping...")
            return False

        if not self.compress:
            in_file = self.path_to_untar / str(file_name).replace('.tar','.fits.fz')
            out_dir = self.path_to_untar

            self.split_ccds(in_file, out_dir)

        return True

    def _write_sbatch(self,
                      sbatch_name: Union[str, Path],
                      qroot: str,
                      jobs_str: str
                      ):
        """
        Write a single Qsub script.

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
                                          ozstar_proj=self.ozstar_proj,
                                          res_str=self.res_str,
                                          jobs_str=jobs_str
                                          )

        sbatch_file = open(sbatch_name, 'w')
        sbatch_file.write(sbatch_text)
        sbatch_file.close()

    def sbatchccds(self,
                   file_name: Path,
                   script_num: int,
                   ccds: List[int]
                   ):
        """
        Write Qsub scripts for all files & submits them to the queue.

        Args:
            file_name: Path to file to be processed.
            script_num: Number identifying which script this is.
            ccds: CCDs to be processed in this sbatch file.

        Returns:
            None
        """

        DECam_root = file_name.stem
        qroot = f'{DECam_root}_q{script_num+1}'

        image_list = [f'{DECam_root}_{f}.jp2' for f in ccds]

        sbatch_name = self.path_to_sbatch / f'{qroot}.sbatch'

        self.logger.info(f"Creating Script: {sbatch_name} "
                         f"for CCDs {min(ccds)} to {max(ccds)}"
                         )
        with importlib.resources.path(
            "dwfprepipe.bin", "prepipe_process_ccd.py"
        ) as process_ccd_script:
            jobs_str_temp = f'{process_ccd_script} ' \
                            f'-i {{0}} ' \
                            f'-d {self.run_date} ' \
                            f'-p {self.path_to_watch} ' \
                            f'-l --local-dir {self.path_to_untar} ' \
                            '&\n'
            if not self.compress:
                self.logger.debug("Adding no-compress to sbatch call")
                jobs_str_temp = jobs_str_temp.replace('&\n', '--no-compress &\n')

        jobs_str = ''
        for image in image_list:
            jobs_str += jobs_str_temp.format(image)

        self._write_sbatch(sbatch_name, qroot, jobs_str)

        if self.dry_run:
            self.logger.info("Dry run selected, not submitting sbatch jobs")
        else:
            if sbatch_name.is_file():
                self.logger.debug(f"Running {sbatch_name}")
                subprocess.run(['sbatch', str(sbatch_name)])
            else:
                logger.critical(f"{sbatch_name} does not exist!")

    def listen(self, warning_time=60):
        """
        Listen for files to process.

        Args:
            warning_time: Number of seconds to wait for a new file before warning the user

        Returns:
            None
        """

        self.logger.info("Now running!")
        self.logger.info(f"Monitoring: {self.path_to_watch}")

        glob_str = '*.tar'
        self.logger.debug(f"Checking files with glob string: {glob_str}")
        before = list(self.path_to_watch.glob(glob_str))
        self.logger.debug(f"Existing files: {before}")
        last_file_time = timer()
        while True:
            after = list(self.path_to_watch.glob(glob_str))
            self.logger.debug(f"Current files: {after}")
            added = [str(f) for f in after if f not in before]
            removed = [str(f) for f in before if f not in after]

            if added:
                last_file_time = timer()
                added_str = ", ".join(added)
                self.logger.info(f"Added: {added_str}")
            if removed:
                removed_str = ", ".join(removed)
                self.logger.info(f"Removed: {removed_str}")

            for i, f in enumerate(added):
                if not wait_for_file(f, wait_time=3):
                    self.logger.info(f'{f} not written in time! Skipping...')
                    continue

                self.process_file(f)
                self.logger.info(f"Finished processing {f}!")
                if i == len(added) - 1:
                    self.logger.info(f"Returning to monitoring {self.path_to_watch}..."
                                     )

            if not added:
                time.sleep(3)
                current_time = timer()
                time_since_file = current_time - last_file_time
                if time_since_file > warning_time:
                    self.logger.warning(f"No new files in {time_since_file:.0f} seconds!")

            before = after
