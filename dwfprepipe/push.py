#!/usr/bin/env python3
#-W error:"WARNING: File may have been truncated:*""
#example usage ./dwf_prepipe.py /fred/oz100/fstars/DWF_Unpack_Test/push/
import os, time
import math
import sys
import glob
import warnings
import argparse
import subprocess
import astropy.io.fits as pyfits

import datetime
import logging
from pathlib import Path



def parse_args():
    # Input Keyword Default Values
    dir_def = "/home4/images/fits/2019B-0253/"
    Qs_def=0.000055
    method_def='p'
    nbundle_def=4
    exp_min_def=-1
    
    
    # Parse Inputs
    parser = argparse.ArgumentParser(description='DWF_Prepipe push script for raw data from CTIO', formatter_class=argparse.RawDescriptionHelpFormatter)
    
    parser.add_argument('-d',
                        '--data_dir',
                        metavar='DIRECTORY',
                        type=str,
                        default=dir_def,
                        help='Directory where tarballs of compressed files are placed'
                        )
    
    parser.add_argument('-q',
                        '--Qs',
                        metavar='DIRECTORY',
                        type=float,
                        default=Qs_def,
                        help='Qstep for fits2jpeg compression'
                        )
    
    parser.add_argument('--method',
                        metavar='PROTOCOL',
                        type=str,
                        default=method_def,
                        help='File Transfer method:(s)erial, (p)arrallel, (b)undle, (l)ist, (e)nd of night'
                        )
    
    parser.add_argument('--nbundle',
                        metavar='NUMBER',
                        type=int,
                        default=nbundle_def,
                        help='Number of Files to bundle together'
                        )
    
    parser.add_argument('--exp_min',
                        metavar='NUMBER',
                        type=int,
                        default=exp_min_def,
                        help='Exposure Number Start for end of night file transfer catchup'
                        )
    
    parser.add_argument('--debug',
                        action="store_true",
                        help='Turn on debug output.'
                        )
    
    parser.add_argument('--quiet',
                        action="store_true",
                        help='Turn off all non-essential debug output'
                        )           

    args = parser.parse_args()
    
    return args

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
            path_to_watch:
            Qs:
            push_method:
            nbundle:
        
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
                      push_dir: Union[str, None] = None,
                      target_dir: Union[str, None] = None
                      ):
        """
        Set the ssh variables and target/destination directories
        
        Args:
            user: account username
            host: destination host
            push_dir: directory to push
            target_dir:
        
        Returns:
            None
        """
        
        self.user = user
        self.host = host
        self.reciever = f'{user}@{host}'
        self.push_dir = push_dir
        self.target_dir = target_dir
        
    def dwf_prepipe_validatefits(self, file_name: Union[str, Path]):
        """
        Validate the fits file
        
        Args:
            file_name: path to the fits file
        
        Returns:
            None
        """
        warnings.filterwarnings('error','.*File may have been truncated:.*',UserWarning)
        
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
        Package a file ready for shipping
        
        Args:
            filepath: path to the file
            
        Returns
            None
        """
        
        filepath = Path(filepath)
        file_name = filepath.name
        
        self.logger.info('Unpacking: {}'.format(file_name))
        
        fz_path = self.data_dir / file_name.with_suffix('.fits.fz')
        
        subprocess.run(['funpack', str(fz_path)])
        
        jp2_dest = self.jp2_dir / file_name
        if not os.path.isdir(os.path.join(jp2_dest)):
            self.logger.info('Creating Directory: {}'.format(jp2_dest))
            os.makedirs(p2_path)
        
        self.logger.info('Compressing: {}'.format(file_name))
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
        self.logger.info('Packaging: {}'.format(packaged_file))
        subprocess.run(['tar',
                        '-cf',
                        str(packaged_file),
                        '-C',
                        str(jp2_path),
                        '.']
                        )

        
    def pushfile(self, filepath: Union[str, Path], parallel: bool = False):
        """
        Push a file to the destination
        
        Args:
            filepath: Path to the file
            parallel: Whether to push in parallel or serial
        
        Returns:
            None
        """

        filepath = Path(filepath)
        
        file_name=filepath.name
        
        tar_path = self.jp2_dir / file_name.with_suffix('.tar')

        self.logger.info(f'Shipping: {tar_path}')
        command="scp "+tar_path+self.reciever+":"+self.push_dir+"; ssh "+self.reciever+" 'mv "+self.push_dir+file_name+".tar "+self.target_dir+"'; rm "+tar_path
        if parallel:
            subprocess.Popen(command,shell=True)
        else:
            subprocess.run(command,shell=True)

    def cleantemp(self, filepath: Union[str, Path]):
        """
        Remove the temporary unpacked .fits; bundler .tar;
        and individual .jp2 files.
        
        Args:
            filepath:
            
        Returns:
            None
        """

        file_name=filepath.name
        
        fits_path = self.data_dir / file_name.with_suffix('.fits')
        
        #remove funpacked .fits file
        self.logger.info(f'Removing: {fits_path}')
        os.remove(fits_path)
        
        #remove excess .tar
        #print('Removing: '+jp2_dir+file_name+'.tar')
        #os.remove(jp2_dir+'.tar')
        
        #Remove .jp2 files
        jp2_path = self.jp2_dir / file_name.with_suffix('.jp2')
        self.logger.info('Cleaning: {}'.format(self.jp2_dir))
        for jp2 in self.jp2_dir.iterdir():
            if jp2.suffix == ".jp2":
                os.remove(self.jp2_dir / jp2)


    def process_endofnight(self):
        """
        Run end-of-night processing
        
        Args:
            None
        
        Returns:
            None
        """

        #Get list of files in remote target directory
        # & list of files in local directory
        get_file_command = f"ssh {self.user}@{self.host} ls {self.target_dir}*.tar"
        remote_list=subprocess.getoutput(get_file_command)
        
        sent_files = []
        for filepath in remote_list.splitlines():
            if filepath.endswith(".tar"):
                file_name = Path(filepath).stem
                sent_files.append(file_name)
        
        obs_list = []
        for f in glob.glob(self.data_dir+'*.fits.fz'):
            obs = Path(f).stem
            obs_list.append(obs)

        obs_list.sort(reverse=True)
        sent_files.sort(reverse=True)

        missing=[f for f in obs_list if not f in sent_files]
        num_missing = len(missing)
        total_obs = len(obs_list)
        perc = 100*len(sent_files)/total_obs

        self.logger.info('Starting end of night transfers...')
        self.logger.info(f'Missing {num_missing} of {total_obs}'
                          ' files ({perc}% successful)'
                         )
    
    

        for i, f in enumerate(missing):
            exp_num=int(f.split('_')[1])
            if exp_num > self.exp_min:
                self.logger.info(f'Processing: {f} ({i} of {num_missing})')
                self.packagefile(f)
                self.pushfile(f)
                self.cleantemp(f)
        
    def process_parallel(self, filelist: list):
        """
        Process a list of files in parallel
        
        Args:
            filelist: List of files to process
            
        Returns:
            None
        """
        
        for f in filelist:
            self.logger.info('Processing: {}'.format((f))
            self.dwf_prepipe_validatefits(f)
            self.packagefile(f)
            self.pushfile(f, parallel=True)
            self.cleantemp(f)
            
    def process_serial(self, filelist: list):
        """
        Process a list of files in serial
        
        Args:
            filelist: List of files to process
            
        Returns:
            None
        """
        file_to_send = filelist[-1]
        self.dwf_prepipe_validatefits(file_to_send)
        self.logger.info('Processing: {}'.format(file_to_send))
        self.packagefile(file_to_send)
        self.pushfile(file_to_send)
        self.cleantemp(f)
        
    def process_bundle(self, filelist: list):
        """
        Process a list of files as bundle
        
        Args:
            filelist: List of files to process
            
        Returns:
            None
        """
        sorted_filelist=sorted(filelist)
        
        if len(sorted_filelist) > self.nbundle:
            bundle=sorted_filelist[-1*self.nbundle:]
        else:
            bundle=sorted_filelist
        
        self.logger.info(['Bundling: {}'.format(f) for f in bundle])
        
        bundle_size = len(bundle)
        
        for i, f in enumerate(bundle):
            self.logger.info('Processing: {}'.format(f))
            self.dwf_prepipe_validatefits(f)
            self.packagefile(f)
            # do all but the last scp in parallel;
            # then force python to wait until the final transfer is complete
            if i < bundlesize:
                self.pushfile(f, parallel=True)
            else:
                self.pushfile(f)
                
            self.cleantemp(f,path_to_watch)

def main():
    start = datetime.datetime.now()
    
    args = parse_args()
    
    logfile = "prepipe_push_{}.log".format(
        start.strftime("%Y%m%d_%H:%M:%S")
    )
    
    logger = get_logger(args.debug, args.quiet, logfile=logfile)

    Push = CTIOPush(args.data_dir, args.Qs, args.method, args.nbundle)
    
    #Begin Monitoring Directory
    self.logger.info('Monitoring: {}'.format(path_to_watch))
    before = dict ([(f, None) for f in glob.glob(path_to_watch+'*.fits.fz')])

    if(method == 'e'):
        Push.process_endofnight()
        return

    while True:
        after = dict ([(f, None) for f in glob.glob(path_to_watch+'*.fits.fz')])
        added = [f for f in after if not f in before]
        removed = [f for f in before if not f in after]

        if added:
            logger.info("Added: {}".format(', '.join(added)))

            if method == 'p':
                Push.process_parallel(added)
            elif method == 's':
                Push.process_serial(added)
            elif method == 'b':
                Push.process_bundle(added)
        
        if removed:
            logger.info("Removed: ".format(', '.join(removed)))
        before = after
        time.sleep (1)
if __name__ == '__main__':
    main()
