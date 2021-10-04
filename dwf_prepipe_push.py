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



def parse_args():
    #Input Keyword Default Values
	dir_def = "/home4/images/fits/2019B-0253/"
	Qs_def=0.000055
	method_def='p'
	nbundle_def=4
	exp_min_def=-1
	
	
	#Parse Inputs
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

	args = parser.parse_args()
	
	return args

class CTIOPush:
    def __init__(self, path_to_watch, Qs, push_method, nbundle):
        self.logger = logging.getLogger('dwf_prepipe.push.CTIOPush')
        self.logger.debug('Created CTIO instance')
        
        self.path_to_watch = path_to_watch
        self.Qs = Qs
        self.push_method = push_method
        self.nbundle = nbundle
        
        self.jp2_dir=os.path.join(self.path_to_watch, 'jp2')
        
        self.set_g2_config()
        
        
    def set_g2_config(self,
                      user='fstars',
                      host='ozstar.swin.edu.au',
                      push_dir=None,
                      target_dir=None
                      ):
        
        self.user = user
	    self.host = host
	    self.reciever = '{}@{}'.format(user, host)
	    self.push_dir=push_dir
	    self.target_dir=target_dir
        
    def dwf_prepipe_validatefits(self, file_name):
	    warnings.filterwarnings('error','.*File may have been truncated:.*',UserWarning)
	    valid=0
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
			    valid=1

    #Package new raw .fits.fz file
    def packagefile(self, file_name):
	    file_name=file_name.split('/')[-1].split('.')[0]
	    
	    self.logger.info('Unpacking: {}'.format(file_name))
	    
	    fz_path = os.path.join(data_dir, '{}.fits.fz'.format(file_name))
	    subprocess.run(['funpack', fz_path])
	    
	    jp2_dest = self.jp2_dir+file_name
	    if not os.path.isdir(os.path.join(jp2_dest)):
		    self.logger.info('Creating Directory: {}'.format(jp2_dest))
		    os.makedirs(p2_path)
	    
	    self.logger.info('Compressing: {}'.format(file_name))
	    fitsfile = file_name+'.fits'
	    jp2file = file_name+'.jp2'
	    
	    subprocess.run(['time',
	                    'f2j_DECam',
	                    '-i',
	                    os.path.join(self.data_dir, fitsfile),
	                    '-o',
	                    os.path.join(jp2_path, jp2file),
	                    'Qstep={}'.format(self.Qs)),
	                    '-num_threads',
	                    '1']
	                    )
	    
	    packaged_file = self.jp2_dir+file_name+'.tar'
	    self.logger.info('Packaging: {}'.format(packaged_file))
	    subprocess.run(['tar',
	                    '-cf',
	                    packaged_file,
	                    '-C',
	                    jp2_path,
	                    '.']
	                    )

    #Parallel Ship file to G2
    def parallel_pushfile(self, file):
	    file_name=file.split('/')[-1].split('.')[0]

	    self.logger.info('Shipping:'+jp2_dir+file_name+'.tar')
	    command="scp "+self.jp2_dir+file_name+".tar "+self.reciever+":"+self.push_dir+"; ssh "+self.reciever+" 'mv "+self.push_dir+file_name+".tar "+self.target_dir+"' ; rm "+self.jp2_dir+file_name+".tar "
	    subprocess.Popen(command,shell=True)
	    self.logger.info('Returning to watch directory')

    #Serial Ship to g2
    def serial_pushfile(self,file):
	    file_name=file.split('/')[-1].split('.')[0]

	    self.logger.info('Shipping:'+self.jp2_dir+file_name+'.tar')
	    command="scp "+self.jp2_dir+file_name+".tar "+self.reciever+":"+self.push_dir+"; ssh "+self.reciever+" 'mv "+self.push_dir+file_name+".tar "+self.target_dir+"'; rm "+self.jp2_dir+file_name+".tar "
	    subprocess.run(command,shell=True)
	    self.logger.info('Returning to watch directory')
	    
    def pushfile(self, file, parallel=False):
        file_name=file.split('/')[-1].split('.')[0]

	    self.logger.info('Shipping:'+self.jp2_dir+file_name+'.tar')
	    command="scp "+self.jp2_dir+file_name+".tar "+self.reciever+":"+self.push_dir+"; ssh "+self.reciever+" 'mv "+self.push_dir+file_name+".tar "+self.target_dir+"'; rm "+self.jp2_dir+file_name+".tar "
	    if parallel:
	        subprocess.Popen(command,shell=True)
	    else:
    	    subprocess.run(command,shell=True)

    def cleantemp(self,file):
	    ##Clean Temperary files - unpacked .fits, bundler .tar and individual .jp2
	    file_name=file.split('/')[-1].split('.')[0]
	    fits_name=file_name+'.fits'
	    
	    #remove funpacked .fits file
	    self.logger.info('Removing: '+self.data_dir+fits_name)
	    os.remove(self.data_dir+fits_name)
	    #remove excess .tar
	    #print('Removing: '+jp2_dir+file_name+'.tar')
	    #os.remove(jp2_dir+'.tar')
	    #Remove .jp2 files
	    self.logger.info('Cleaning: '+self.jp2_dir+'/')
	    [os.remove(self.jp2_dir+'/'+jp2) for jp2 in os.listdir(self.jp2_dir) if jp2.endswith(".jp2")]

    def process_endofnight(self):

	    #Get list of files in remote target directory & list of files in local directory
	    remote_list=subprocess.getoutput("ssh "+self.user+"@"+self.host+" 'ls "+self.target_dir+"*.tar'")
	    sent_files=[file.split('/')[-1].split('.')[0] for file in remote_list.splitlines() if file.endswith(".tar")]
	    obs_list=[f.split('/')[-1].split('.')[0] for f in glob.glob(data_dir+'*.fits.fz')]

	    obs_list.sort(reverse=True)
	    sent_files.sort(reverse=True)

	    missing=[f for f in obs_list if not f in sent_files]
	    num_missing = len(missing)

	    self.logger.info('Starting end of night transfers for general completion')
	    self.logger.info('Missing Files: '+str(len(missing))+'/'+str(len(obs_list))+' ('+str(len(sent_files))+' sent)')
	
	

	    for i, f in enumerate(missing):
		    exp=int(f.split('_')[1])
		    if(exp > self.exp_min):
			    self.logger.info('Processing: {} ({} of {})'.format(f, i, num_missing)
			    self.packagefile(f)
			    #self.serial_pushfile(f)
			    self.pushfile(f)
			    self.cleantemp(f)
        
    def process_parallel(filelist):
        for f in filelist:
	        self.logger.info('Processing: '+f)
	        self.dwf_prepipe_validatefits(f)
	        self.packagefile(f)
	        self.pushfile(f, parallel=True)
	        self.cleantemp(f)
	        
    def process_serial(filelist):
        file_to_send = filelist[-1]
        self.dwf_prepipe_validatefits(file_to_send)
        self.logger.info('Processing: {}'.format(file_to_send))
        self.packagefile(file_to_send)
        #self.serial_pushfile(file_to_send)
        self.pushfile(file_to_send)
        self.cleantemp(f)
        
    def process_bundle(filelist):
        sorted_filelist=sorted(filelist)
        
        if len(sorted_filelist) > self.nbundle:
	        bundle=sorted_filelist[-1*self.nbundle:]
        else:
	        bundle=sorted_filelist
        
        self.logger.info(['Bundling:'+str(f) for f in bundle])
        
        bundle_size = len(bundle)
        
        for i, f in enumerate(bundle):
	        self.logger.info('Processing: '+f)
	        self.dwf_prepipe_validatefits(f)
	        self.packagefile(f)
	        #do all but the last scp in parallel; then force python to wait until the final transfer is complete
	        if i < bundlesize:
	            self.pushfile(f, parallel=True)
	        else:
		        self.serial_pushfile(f)
		        
	        self.cleantemp(f,path_to_watch)

def main():
	
	args = parse_args()

	Push = CTIOPush(args.data_dir, args.Qs, args.method, args.nbundle)
	
	#Begin Monitoring Directory
	self.logger.info('Monitoring:'+path_to_watch)
	before = dict ([(f, None) for f in glob.glob(path_to_watch+'*.fits.fz')])

	if(method == 'e'):
		Push.process_endofnight()
		return

	while True:
        after = dict ([(f, None) for f in glob.glob(path_to_watch+'*.fits.fz')])
		added = [f for f in after if not f in before]
		removed = [f for f in before if not f in after]

		if added:
		    print("Added: ", ", ".join (added))

		    if method == 'p':
			    Push.process_parallel(added)
            elif method == 's':
			    Push.process_serial(added)
            elif method == 'b':
			    Push.process_bundle(added)
		
		if removed:
		    print("Removed: ", ", ".join (removed))
		before = after
		time.sleep (1)
if __name__ == '__main__':
    main()
