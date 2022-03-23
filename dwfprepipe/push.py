import time
import glob
import subprocess
import logging
import multiprocessing as mp

from pathlib import Path
from typing import Union
from dwfprepipe.utils import wait_for_file


class CTIOPushInitError(Exception):
    """
    A defined error for a problem encountered in the CTIOPush initialisation.
    """
    pass


class CTIOPush:
    def __init__(self,
                 path_to_watch: Union[str, Path],
                 Qs: float,
                 push_method: str,
                 nbundle: int
                 ):
        """
        Constructor method.

        Args:
            path_to_watch: Path to directory containing files to push.
            Qs: Compression ratio/number/???.
            push_method: Method to push data with.
            nbundle: Number of files to bundle together. Only relevant if
                `push_method` is set to `bundle`.

        Returns:
            None

        Raises:
            CTIOPushInitError: Problems found in the requested settings.
        """

        self.logger = logging.getLogger('dwf_prepipe.push.CTIOPush')

        self.valid_methods = {'s': 'serial',
                              'p': 'parallel',
                              'b': 'bundle',
                              'e': 'end of night',
                              'c': 'compression split'
                              }

        if push_method in self.valid_methods.keys():
            push_method = self.valid_methods[push_method]

        self.path_to_watch = Path(path_to_watch)
        self.Qs = Qs

        self.push_method = push_method
        self.nbundle = nbundle

        self.jp2_dir = self.path_to_watch / 'jp2'

        self.set_ssh_config()

        valid_settings = self._validate_settings()
        if not valid_settings:
            raise CTIOPushInitError("Problems found in the requested "
                                    "settings! Please address and try again."
                                    )

        self.logger.info("Successfully initiated CTIOPush instance!")
        self.logger.info(f"Watching {self.path_to_watch}...")
        self.logger.info(f"Will transfer with {self.push_method} protocol...")
        self.logger.debug(f"Running with Qs={self.Qs}")
        self.logger.debug(f"Running with nbundle={self.nbundle}")
        self.logger.debug(f"Running with jp2_dir={self.jp2_dir}")

    def _validate_settings(self):
        """
        Validate the requested settings

        Args:
            None

        Returns:
            bool
        """

        valid = True

        if self.push_method not in self.valid_methods.values():
            self.logger.critical(
                "The push method must be one of the following: "
                "(s)erial, (p)arallel, (b)undle, (e)nd of night, "
                "(c)ompression split. "
                "Please choose one of the above and try again."
            )
            valid = False

        if not self.path_to_watch.is_dir():
            self.logger.critical(f"The provided path to watch, "
                                 f"{self.path_to_watch}, does not exist!"
                                 )
            valid = False

        if not self.jp2_dir.is_dir():
            self.logger.critical(f"The provided jp2 directory, "
                                 f"{self.jp2_dir}, does not exist!"
                                 )
            valid = False

        if not self.push_dir.is_dir():
            self.logger.critical(f"The provided push directory, "
                                 f"{self.push_dir}, does not exist!"
                                 )
            valid = False

        if not self.target_dir.is_dir():
            self.logger.critical(f"The provided target directory, "
                                 f"{self.target_dir}, does not exist!"
                                 )
            valid = False

        return valid

    def set_ssh_config(self,
                       user: str = 'fstars',
                       host: str = 'ozstar.swin.edu.au',
                       push_dir: Union[str, Path] =
                       '/fred/oz100/fstars/push/',
                       target_dir: Union[str, Path] =
                       '/fred/oz100/fstars/DWF_Unpack_Test/push/'
                       ):
        """
        Set the ssh variables and target/destination directories.

        Args:
            user: Account username.
            host: Destination host.
            push_dir: Directory to push to.
            target_dir: Directory to move files to after push.

        Returns:
            None
        """

        self.logger.debug(f"Setting user to {user}")
        self.user = user
        self.logger.debug(f"Setting host to {host}")
        self.host = host
        self.reciever = f'{user}@{host}'
        self.logger.debug(f"Setting reciever to {self.reciever}")
        self.logger.debug(f"Setting push_dir to {push_dir}")
        self.push_dir = Path(push_dir)
        self.logger.debug(f"Setting target_dir to {target_dir}")
        self.target_dir = Path(target_dir)

    # Package new raw .fits.fz file
    def packagefile(self, filepath: Union[str, Path]):
        """
        Package a file ready for shipping.

        Args:
            filepath: Path to the file to be packaged.

        Returns
            None
        """

        filepath = Path(filepath)
        file_name = filepath.name

        self.logger.info(f'Unpacking: {file_name}')

        fz_path = self.data_dir / file_name.with_suffix('.fits.fz')

        subprocess.run(['funpack', str(fz_path)])

        jp2_dest = self.jp2_dir / file_name
        if not jp2_dest.is_dir():
            self.logger.info(f'Creating Directory: {jp2_dest}')
            jp2_dest.mkdir()

        self.logger.info(f'Compressing: {file_name}')
        fitsfile = file_name.with_suffix('.fits')
        jp2file = file_name.with_suffix('.jp2')

        subprocess.run(['time',
                        'f2j_DECam',
                        '-i',
                        str(self.data_dir / fitsfile),
                        '-o',
                        str(self.jp2_dir / jp2file),
                        f'Qstep={self.Qs}',
                        '-num_threads',
                        '1']
                       )

        packaged_file = self.jp2_dir / file_name.with_suffix('.tar')
        self.logger.info(f'Packaging: {packaged_file}')
        subprocess.run(['tar',
                        '-cf',
                        str(packaged_file),
                        '-C',
                        str(jp2_path),
                        '.']
                       )

    def pushfile(self, filepath: Union[str, Path], parallel: bool = False):
        """
        Push a file to the destination.

        Args:
            filepath: Path to the file to be pushed.
            parallel: If `True`, use parallel push, else use serial push.

        Returns:
            None
        """

        filepath = Path(filepath)

        file_name = filepath.name

        tar_path = self.jp2_dir / file_name.with_suffix('.tar')

        self.logger.info(f'Shipping: {tar_path}')

        command = (f"scp {tar_path} {self.reciever}:{self.push_dir}; "
                   f"ssh {self.reciever} "
                   f"'mv {self.push_dir / file_name}.tar {self.target_dir}'; "
                   f"rm {tar_path}"
                   )

        if parallel:
            subprocess.Popen(command, shell=True)
        else:
            subprocess.run(command, shell=True)

    def cleantemp(self, filepath: Union[str, Path]):
        """
        Remove the temporary unpacked .fits; bundler .tar;
        and individual .jp2 files.

        Args:
            filepath: Path of the temp file to be cleaned.

        Returns:
            None
        """

        file_name = filepath.name

        fits_path = self.data_dir / file_name.with_suffix('.fits')

        # remove funpacked .fits file
        self.logger.info(f'Removing: {fits_path}')
        fits_path.unlink()

        # Remove .jp2 files
        jp2_path = self.jp2_dir / file_name.with_suffix('.jp2')
        self.logger.info(f'Cleaning: {self.jp2_dir}')
        for jp2 in self.jp2_dir.iterdir():
            if jp2.suffix == ".jp2":
                jp2.unlink()

    def process_endofnight(self, exp_min):
        """
        Run end-of-night processing.

        Args:
            exp_min: The first exposure number to process.

        Returns:
            None
        """

        # Get list of files in remote target directory
        # & list of files in local directory
        get_file_command = (f"ssh {self.user}@{self.host}"
                            f"ls {self.target_dir}*.tar"
                            )
        remote_list = subprocess.getoutput(get_file_command)

        sent_files = []
        for filepath in remote_list.splitlines():
            if filepath.endswith(".tar"):
                file_name = Path(filepath).stem
                sent_files.append(file_name)

        obs_list = []
        for f in self.data_dir.glob('*.fits.fz'):
            obs = Path(f).stem
            obs_list.append(obs)

        obs_list.sort(reverse=True)
        sent_files.sort(reverse=True)

        missing = [f for f in obs_list if f not in sent_files]
        num_missing = len(missing)
        total_obs = len(obs_list)
        perc = 100 * len(sent_files) / total_obs

        self.logger.info('Starting end of night transfers...')
        self.logger.info(f'Missing {num_missing} of {total_obs} '
                         f'files ({perc}% successful)'
                         )

        for i, f in enumerate(missing):
            exp_num = int(f.split('_')[1])
            if exp_num > exp_min:
                self.logger.info(f'Processing: {f} ({i} of {num_missing})')
                self.packagefile(f)
                self.pushfile(f)
                self.cleantemp(f)

    def push_parallel(self, filelist: list):
        """
        Process a list of files in parallel.

        Args:
            filelist: List of files to process.

        Returns:
            None
        """

        for f in filelist:
            self.logger.info(f'Push: {f}...')

            if not wait_for_file(f):
                self.logger.info(f'{f} not written in time! Skipping...')
                continue

            self.pushfile(f, parallel=True)
            self.cleantemp(f)

    def push_serial(self, filename: Union[str, Path]):
        """
        Process a file in serial.

        Args:
            filename: Path to file to be processed.

        Returns:
            None
        """

        self.logger.info(f'Push: {filename}...')

        if not wait_for_file(filename):
            self.logger.info(f'{filename} not written in time! Skipping...')
            return
        
        self.pushfile(filename)
        self.cleantemp(filename)

    def push_bundle(self, filelist: list):
        """
        Process a list of files as bundle.

        Args:
            filelist: List of files to process.

        Returns:
            None
        """
        sorted_filelist = sorted(filelist)

        if len(sorted_filelist) > self.nbundle:
            bundle = sorted_filelist[-1 * self.nbundle:]
        else:
            bundle = sorted_filelist

        self.logger.info(f"Bundling: {', '.join(bundle)}...")

        bundle_size = len(bundle)

        for i, f in enumerate(bundle):
            self.logger.info(f'Pushing: {f}...')

            if not wait_for_file(f):
                self.logger.info(f'{f} not written in time! Skipping...')
                continue

            # do all but the last scp in parallel;
            # then force python to wait until the final transfer is complete
            if i < bundle_size:
                self.pushfile(f, parallel=True)
            else:
                self.pushfile(f)

            self.cleantemp(f, self.path_to_watch)
        
    def listen(self):
        """
        Listen for new images, package and push them simultaneously.
    
        Args:
            None
    
        Returns:
            None
        """
        self.logger.info("Now running!")
        
        p1 = mp.Process(target=self.listenfor,args='Packaging')
        p1.start()
        
        p2 = mp.Process(target=self.listenfor,args='Pushing')
        p2.start()
        
    def listenfor(self,process: str):
        """
        Listen for new images, push or package them.
    
        Args:
            Process: Name of process we're listening for - either pushing or
                     packaging.
    
        Returns:
            None
        """
        
        if process == 'Packaging':
            self.logger.info(f"Monitoring: {self.path_to_watch}")
            glob_str = str(self.path_to_watch) + '/*.fits.fz'
        elif process == 'Pushing':
            self.logger.info(f"Monitoring: {self.jp2_dir}")
            glob_str = str(self.jp2_dir) + '/*.tar'
    
        before = glob.glob(glob_str)
    
        while True:
            after = glob.glob(glob_str)
            added = [f for f in after if f not in before]
            removed = [f for f in before if f not in after]
    
            if added:
                self.logger.info("Added: {}".format(', '.join(added)))
                if process == 'Packaging':
                    for f in added:
                        self.packagefile(f)
                elif process == 'Pushing':
                    if self.method == 'parallel':
                        self.push_parallel(added)
                    elif self.method == 'serial':
                        self.push_serial(added[-1])
                    elif self.method == 'bundle':
                        self.push_bundle(added)
    
            if removed:
                removed_str = ', '.join(removed)
                self.logger.info(f"Removed: {removed_str}")
    
            before = after
    
            time.sleep(1)
