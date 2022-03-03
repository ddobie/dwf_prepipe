#!/usr/bin/env python3

import os, time
import math
import sys
import glob
import warnings
import subprocess
import astropy.io.fits as pyfits

import datetime
import logging
from pathlib import Path
from typing import Union


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
            Exception: Invalid `push_method`
        """
        
        valid_methods = {'s': 'serial',
                         'p': 'parallel',
                         'b': 'bundle',
                         'l':'list',
                         'e':'end of night',
                         }
        self.logger = logging.getLogger('dwf_prepipe.push.CTIOPush')
        
        if push_method in valid_methods.keys():
            push_method = valid_methods[push_method]
        
        if push_method not in valid_methods.values():
            raise Exception(
                "The push method must be one of the following:"
                "(s)erial, (p)arrallel, (b)undle, (l)ist, (e)nd of night."
                "Please choose one of the above and try again."
                )
        
        self.logger.info('Created CTIOPush instance')
        
        self.path_to_watch = Path(path_to_watch)
        self.logger.info(f"Watching {path_to_watch}...")
        
        self.Qs = Qs
        self.logger.info(f"Compressing with Qs={Qs}...")
        
        self.push_method = push_method
        self.logger.info(f"Will transfer with {push_method} protocol...")
        self.nbundle = nbundle
        
        self.jp2_dir= self.path_to_watch / 'jp2'
        
        self.set_ssh_config()
        
        
    def set_ssh_config(self,
                      user: str = 'fstars',
                      host: str = 'ozstar.swin.edu.au',
                      push_dir: Union[str, Path] = '/fred/oz100/fstars/push/',
                      target_dir: Union[str, Path] = '/fred/oz100/fstars/DWF_Unpack_Test/push/'
                      ):
        """
        Set the ssh variables and target/destination directories.
        
        Args:
            user: Account username.
            host: Destination host
            push_dir: Directory to push to.
            target_dir: Directory to move files to after push.
        
        Returns:
            None
        """
        
        self.user = user
        self.host = host
        self.reciever = f'{user}@{host}'
        self.push_dir = Path(push_dir)
        self.target_dir = Path(target_dir)
        
    def dwf_prepipe_validatefits(self, file_name: Union[str, Path]):
        """
        Validate the fits file
        
        Args:
            file_name: path to the fits file
        
        Returns:
            None
        """
        warnings.filterwarnings('error',
                                '.*File may have been truncated:.*',
                                UserWarning
                                )
        
        valid=False
        
        while(not valid):
            try:
                test=pyfits.open(file_name)
            except OSError:
                self.logger.error('{} still writing...'.format(file_name))
                time.sleep(3)
            except UserWarning:
                self.logger.error('{} still writing...'.format(file_name))
                time.sleep(0.5)
            except IOError:
                self.logger.error('{} still writing...'.format(file_name))
                time.sleep(0.5)
            else:
                self.logger.info('{} passes validation'.format(file_name))
                valid=True

    #Package new raw .fits.fz file
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
        
        file_name=filepath.name
        
        tar_path = self.jp2_dir / file_name.with_suffix('.tar')

        self.logger.info(f'Shipping: {tar_path}')
        
        command=(f"scp {tar_path} {self.reciever}:{self.push_dir}; "
                f"ssh {self.reciever} "
                f"'mv {self.push_dir / file_name}.tar {self.target_dir}'; "
                f"rm {tar_path}"
                )
        
        if parallel:
            subprocess.Popen(command,shell=True)
        else:
            subprocess.run(command,shell=True)

    def cleantemp(self, filepath: Union[str, Path]):
        """
        Remove the temporary unpacked .fits; bundler .tar;
        and individual .jp2 files.
        
        Args:
            filepath: Path of the temp file to be cleaned.
            
        Returns:
            None
        """

        file_name=filepath.name
        
        fits_path = self.data_dir / file_name.with_suffix('.fits')
        
        #remove funpacked .fits file
        self.logger.info(f'Removing: {fits_path}')
        fits_path.unlink()
        
        #remove excess .tar
        #print('Removing: '+jp2_dir+file_name+'.tar')
        #os.remove(jp2_dir+'.tar')
        
        #Remove .jp2 files
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

        #Get list of files in remote target directory
        # & list of files in local directory
        get_file_command = f"ssh {self.user}@{self.host}"
                           f"ls {self.target_dir}*.tar"
        remote_list=subprocess.getoutput(get_file_command)
        
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

        missing=[f for f in obs_list if not f in sent_files]
        num_missing = len(missing)
        total_obs = len(obs_list)
        perc = 100*len(sent_files)/total_obs

        self.logger.info('Starting end of night transfers...')
        self.logger.info(f'Missing {num_missing} of {total_obs} '
                          f'files ({perc}% successful)'
                         )
    
    

        for i, f in enumerate(missing):
            exp_num=int(f.split('_')[1])
            if exp_num > exp_min:
                self.logger.info(f'Processing: {f} ({i} of {num_missing})')
                self.packagefile(f)
                self.pushfile(f)
                self.cleantemp(f)
        
    def process_parallel(self, filelist: list):
        """
        Process a list of files in parallel.
        
        Args:
            filelist: List of files to process.
            
        Returns:
            None
        """
        
        for f in filelist:
            self.logger.info(f'Processing: {f}')
            self.dwf_prepipe_validatefits(f)
            self.packagefile(f)
            self.pushfile(f, parallel=True)
            self.cleantemp(f)
            
    def process_serial(self, filename: Union[str, Path]):
        """
        Process a file in serial
        
        Args:
            filename: Path to file to be processed.
            
        Returns:
            None
        """
        self.dwf_prepipe_validatefits(filename)
        self.logger.info(f'Processing: {filename}')
        self.packagefile(filename)
        self.pushfile(filename)
        self.cleantemp(filename)
        
    def process_bundle(self, filelist: list):
        """
        Process a list of files as bundle.
        
        Args:
            filelist: List of files to process.
            
        Returns:
            None
        """
        sorted_filelist=sorted(filelist)
        
        if len(sorted_filelist) > self.nbundle:
            bundle=sorted_filelist[-1*self.nbundle:]
        else:
            bundle=sorted_filelist
        
        self.logger.info(f"Bundling: {', '.join(bundle)}")
        
        bundle_size = len(bundle)
        
        for i, f in enumerate(bundle):
            self.logger.info(f'Processing: {f}')
            self.dwf_prepipe_validatefits(f)
            self.packagefile(f)
            # do all but the last scp in parallel;
            # then force python to wait until the final transfer is complete
            if i < bundlesize:
                self.pushfile(f, parallel=True)
            else:
                self.pushfile(f)
                
            self.cleantemp(f,self.path_to_watch)
    
    def listen(self):
        """
        Run
        
        Args:
            None
            
        Returns:
            None
        """

        self.logger.info("Now running!")
        self.logger.info(f"Monitoring: {self.path_to_watch}")
        
        glob_str = str(self.path_to_watch)+'*.fits.fz'
        
        before = glob.glob(glob_str)

        while True:
            after = glob.glob(glob_str)
            added = [f for f in after if not f in before]
            removed = [f for f in before if not f in after]

            if added:
                logger.info("Added: {}".format(', '.join(added)))

                if self.method == 'parallel':
                    self.process_parallel(added)
                elif self.method == 'serial':
                    self.process_serial(added[-1])
                elif self.method == 'bundle':
                    self.process_bundle(added)
            
            if removed:
                removed_str = ', '.join(removed)
                logger.info(f"Removed: {removed_str}")

            before = after

            time.sleep(1)
        


