'''
This program looks into a given directory and coadds
biases and flat fields into master calibration files.
'''

__whatami__ = 'Create master bias and flat field frames.'
__author__ = 'Igor Andreoni <andreoni@caltech.edu>'

def stack_average(imlist):
    array_all = []
    for imname in imlist:
        hdulist = fits.open(imname)
        if imname == imlist[0]:
            #Get an header to propagate into the master bias
            header = hdulist[0].header
        array_all.append(hdulist[0].data)

    return np.mean(array_all, axis = 0), header


def create_master_bias(path_in, path_out):
    import glob

    ''' Create a master bias '''
    bias_imlist = glob.glob(f"{path_in}/bias*")

    #Verbose
    if bias_imlist == []:
        print("Hey, there are no bias.* frames in {path_in}")
        print("Master bias not created.")
        return
    print('Creating master bias, using:')
    for b in bias_imlist:
        print(b)
    
    stack_bias, header_bias = stack_average(bias_imlist)
    hdu_bias = fits.PrimaryHDU(stack_bias, header = header_bias)
    hdul_bias = fits.HDUList([hdu_bias])
    hdul_bias.writeto(f"{path_out}/bias.master.fits", overwrite = True)
    print(f"Created master bias {path_out}/bias.master.fits")

    return


def create_master_flat(path_in, path_out):
    import glob

    ''' Create a master flat (one per filter) '''
    flat_imlist = glob.glob(f"{path_in}/domeflat*")
    #Verbose
    if flat_imlist == []:
        print(f"Hey, there are no domeflat.* frames in {path_in}")
        print("Master flat not created.")
        return
    
    #Which filters are available?
    filters_available = set(f.split('.')[1] for f in flat_imlist)
    print(f"Flats available for the following filters: {filters_available}")

    for fil in filters_available:    
        flat_imlist_filter = glob.glob(f"{path_in}/domeflat.{fil}.*")
        print(f"Creating {fil}-band master flat using:")
        for ff in flat_imlist_filter:
            print(ff)
        stack_flat, header_flat = stack_average(flat_imlist_filter)
        hdu_flat = fits.PrimaryHDU(stack_flat, header = header_flat)
        hdul_flat = fits.HDUList([hdu_flat])
        hdul_flat.writeto(f"{path_out}/domeflat.{fil}.master.fits", overwrite = True)
        print(f"Created {fil}-band master flat {path_out}/domeflat.{fil}.master.fits")

    return

def str2bool(v):
    if v.lower() in ('yes', 'true', 't', 'y', '1', 'Yes', 'True'):
        return True
    elif v.lower() in ('no', 'false', 'f', 'n', '0', 'No', 'False'):
        return False
    else:
        raise argparse.ArgumentTypeError('Boolean value expected.')


if __name__ == '__main__':
    from argparse import ArgumentParser
    import pdb
    
    parser = ArgumentParser()
    parser.add_argument('--i', required=True, 
                        help='Directory where the calibration files are stored. ',\
                        dest='path_in', nargs=1)

    parser.add_argument('--o', required=False, 
                        help='Directory where the master calibration files will be \
			created.', dest='path_out', nargs='+')

    parser.add_argument('--a', dest='all', type=str2bool, required=False, \
                help='Create the master calib files for all CCDs')

    args = parser.parse_args()

    from astropy.io import fits
    import numpy as np

    if args.path_out == None:
        path_out = args.path_in
    else:
        path_out = args.path_out

    #Check that the paths are not lists
    if isinstance(args.path_in, (list,)):
        path_in = args.path_in[0]
    if isinstance(path_out, (list,)):
        path_out = path_out[0]

#If --a, then apply to all CCDs
    if args.all:
        i=1
        while i < 62:
            path_ccd = '/'.join(args.path_in[0].split('/')[0:-2])+f"/{i}/"
            create_master_bias(path_ccd, path_ccd)
            create_master_flat(path_ccd, path_ccd)
            i=i+1
    else:
        create_master_bias(args.path_in, path_out)
        create_master_flat(args.path_in, path_out)

