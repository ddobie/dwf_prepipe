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

def dwf_prepipe_validatefits(file_name,data_dir):
	warnings.filterwarnings('error','.*File may have been truncated:.*',UserWarning)
	valid=0
	while(not valid):
		try:
			test=pyfits.open(file_name)
		except OSError:
			print('OS Error:')
			print(file_name+' still writing ...')
			time.sleep(3)
		except UserWarning:
			print('User Warning: ')
			print(file_name+' still writing ...')
			time.sleep(0.5)
		except IOError:
			print('IO Error:')
			print(file_name+' still writing ...')
			time.sleep(0.5)
		else:
			print(file_name+' pass!')
			valid=1

#Package new raw .fits.fz file
def dwf_prepipe_packagefile(file,data_dir,Qs):
	file_name=file.split('/')[-1].split('.')[0]
	jp2_dir=data_dir+"jp2/"
	print('Unpacking:'+file_name)
	print(file_name)
	print(data_dir+file_name+'.fits.fz')
	subprocess.run(['funpack',data_dir+file_name+'.fits.fz'])
	if not os.path.isdir(jp2_dir+file_name):
		print('Creating Directory: '+jp2_dir+file_name)
		os.makedirs(jp2_dir+file_name)
	print('Compressing:'+file_name)
	subprocess.run(['time','f2j_DECam','-i',data_dir+file_name+'.fits','-o',jp2_dir+file_name+'/'+file_name+'.jp2','Qstep='+str(Qs),'-num_threads',str(1)])
	print('Packaging:'+jp2_dir+file_name+'.tar')
	subprocess.run(['tar','-cf',jp2_dir+file_name+'.tar','-C',jp2_dir+file_name+'/','.'])

#Parallel Ship file to G2
def dwf_prepipe_parallel_pushfile(file,data_dir):
	file_name=file.split('/')[-1].split('.')[0]

	#g2 configuration
	user='fstars'
	host='ozstar.swin.edu.au'
	reciever=user+'@'+host
	push_dir='/fred/oz100/fstars/push/'
	target_dir='/fred/oz100/fstars/DWF_Unpack_Test/push/'

	jp2_dir=data_dir+"jp2/"

	print('Shipping:'+jp2_dir+file_name+'.tar')
	command="scp "+jp2_dir+file_name+".tar "+reciever+":"+push_dir+"; ssh "+reciever+" 'mv "+push_dir+file_name+".tar "+target_dir+"' ; rm "+jp2_dir+file_name+".tar "
	subprocess.Popen(command,shell=True)
	print('Returning to watch directory')

#Serial Ship to g2
def dwf_prepipe_serial_pushfile(file,data_dir):
	file_name=file.split('/')[-1].split('.')[0]
	#g2 configuration
	user='fstars'
	host='ozstar.swin.edu.au'
	reciever=user+'@'+host
	push_dir='/fred/oz100/fstars/push/'
	target_dir='/fred/oz100/fstars/DWF_Unpack_Test/push/'

	jp2_dir=data_dir+"jp2/"

	print('Shipping:'+jp2_dir+file_name+'.tar')
	command="scp "+jp2_dir+file_name+".tar "+reciever+":"+push_dir+"; ssh "+reciever+" 'mv "+push_dir+file_name+".tar "+target_dir+"'; rm "+jp2_dir+file_name+".tar "
	subprocess.run(command,shell=True)
	print('Returning to watch directory')

def dwf_prepipe_cleantemp(file,data_dir):
	##Clean Temperary files - unpacked .fits, bundler .tar and individual .jp2
	file_name=file.split('/')[-1].split('.')[0]
	jp2_dir=data_dir+"jp2/"+file_name
	fits_name=file_name+'.fits'
	#remove funpacked .fits file
	print('Removing: '+data_dir+fits_name)
	os.remove(data_dir+fits_name)
	#remove excess .tar
	#print('Removing: '+jp2_dir+file_name+'.tar')
	#os.remove(jp2_dir+'.tar')
	#Remove .jp2 files
	print('Cleaning: '+jp2_dir+'/')
	[os.remove(jp2_dir+'/'+jp2) for jp2 in os.listdir(jp2_dir) if jp2.endswith(".jp2")]

def dwf_prepipe_endofnight(data_dir,exp_min,Qs):
	user='fstars'
	host='ozstar.swin.edu.au'
	target_dir='/fred/oz100/fstars/DWF_Unpack_Test/push/'

	#Get list of files in remote target directory & list of files in local directory
	remote_list=subprocess.getoutput("ssh "+user+"@"+host+" 'ls "+target_dir+"*.tar'")
	sent_files=[file.split('/')[-1].split('.')[0] for file in remote_list.splitlines() if file.endswith(".tar")]
	obs_list=[f.split('/')[-1].split('.')[0] for f in glob.glob(data_dir+'*.fits.fz')]

	obs_list.sort(reverse=True)
	sent_files.sort(reverse=True)

	missing=[f for f in obs_list if not f in sent_files]
	num_missing = len(missing)

	print('Starting end of night transfers for general completion')
	print('Missing Files: '+str(len(missing))+'/'+str(len(obs_list))+' ('+str(len(sent_files))+' sent)')
	
	

	for i, f in enumerate(missing):
		exp=int(f.split('_')[1])
		if(exp > exp_min):
			print('Processing: {} ({} of {})'.format(f, i, num_missing)
			dwf_prepipe_packagefile(f,data_dir,Qs)
			dwf_prepipe_serial_pushfile(f,data_dir)
			dwf_prepipe_cleantemp(f,data_dir)

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


def process_parallel(filelist):
    for f in filelist:
	    print('Processing: '+f)
	    dwf_prepipe_validatefits(f,path_to_watch)
	    dwf_prepipe_packagefile(f,path_to_watch,Qs)
	    dwf_prepipe_parallel_pushfile(f,path_to_watch)
	    dwf_prepipe_cleantemp(f,path_to_watch)
	    
def process_serial(filelist):
    dwf_prepipe_validatefits(filelist[-1],path_to_watch)
    print('Processing: '+filelist[-1])
    dwf_prepipe_packagefile(filelist[-1],path_to_watch,Qs)
    dwf_prepipe_serial_pushfile(filelist[-1],path_to_watch)
    dwf_prepipe_cleantemp(f,path_to_watch)
    
def process_bundle(filelist):
    sorted_filelist=sorted(filelist)
    
    if len(sorted_filelist) > nbundle:
	    bundle=sorted_filelist[-1*nbundle:]
    else:
	    bundle=sorted_filelist
    
    print(['Bundling:'+str(f) for f in bundle])
    
    bundle_size = len(bundle)
    
    for i, f in enumerate(bundle):
	    print('Processing: '+f)
	    dwf_prepipe_validatefits(f,path_to_watch)
	    dwf_prepipe_packagefile(f,path_to_watch,Qs)
	    #do all but the last scp in parallel; then force python to wait until the final transfer is complete
	    if i < bundlesize:
	        dwf_prepipe_parallel_pushfile(f,path_to_watch)
	    else:
		    dwf_prepipe_serial_pushfile(f,path_to_watch)
		    
	    dwf_prepipe_cleantemp(f,path_to_watch)

def main():
	
	args = parse_args()

	path_to_watch=args.data_dir
	Qs=args.Qs
	method=args.method
	nbundle=args.nbundle
	
	#Begin Monitoring Directory
	print('Monitoring:'+path_to_watch)
	before = dict ([(f, None) for f in glob.glob(path_to_watch+'*.fits.fz')])

	if(method == 'e'):
		dwf_prepipe_endofnight(path_to_watch, exp_min, Qs)
		return

	while True:
        after = dict ([(f, None) for f in glob.glob(path_to_watch+'*.fits.fz')])
		added = [f for f in after if not f in before]
		removed = [f for f in before if not f in after]

		if added:
		    print("Added: ", ", ".join (added))

		    if method == 'p':
			    process_parallel(added)
            elif method == 's':
			    process_serial(filelist)
            elif method == 'b':
			    process_bundle(filelist)
		
		if removed:
		    print("Removed: ", ", ".join (removed))
		before = after
		time.sleep (1)
if __name__ == '__main__':
    main()
