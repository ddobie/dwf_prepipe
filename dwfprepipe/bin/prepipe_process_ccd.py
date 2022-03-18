#!/usr/bin/env python3
import os
import datetime
import shutil
import argparse
import subprocess
import glob
import importlib.resources
# ~/.astropy/config/astropy.cfg was getting messed up -
# seperate default (used by pipeloop?) and this
# os.environ['XDG_CONFIG_HOME']='/home/fstars/.python3_config/'

import astropy.io.fits as pyfits

from dwfprepipe.utils import get_logger
from pathlib import Path


def check_path(path):
    """Check if path ends with a slash ('/'). Else, it adds a slash.

    The function also creates the directory if it does not existing.

    Parameters
    ----------
    path : str
        A path

    Returns
    -------
    path : str
        A functional path
    """
    from os import makedirs
    from os.path import exists

    if len(path) > 0 and path[-1] != '/':
        path = path + '/'

    if not exists(path):
        makedirs(path)

    return path


def check_wcs(ut, ccd, expnum):
    pipeloop_out = subprocess.check_output(['pipeview.pl',
                                            '-red',
                                            ut,
                                            ccd,
                                            '-stage',
                                            'WCSNON',
                                            '-id', str(expnum)
                                            ],
                                           stderr=subprocess.STDOUT,
                                           universal_newlines=True)

    wcs_val = pipeloop_out.splitlines()[9].strip(' \t\n\r').split(' ')[-1]
    return (wcs_val == 'X')


def get_shift_ccd(ut, ccd, Field, expnum):
    pipeview_out = subprocess.check_output(['pipeview.pl',
                                            '-red',
                                            ut,
                                            ccd,
                                            '-stage',
                                            'WCSNON',
                                            '-wcs',
                                            '-im',
                                            Field
                                            ],
                                           stderr=subprocess.STDOUT,
                                           universal_newlines=True
                                           )

    pipeview_out = pipeview_out.splitlines()[2:]

    rashift = []
    decshift = []

    ra_close = []
    dec_close = []

    for line in pipeview_out:
        # Checks Valid Line
        if((line.split()[0].split('.')[0] == Field) and
                (line.split()[1] == '1')):

            rashift.append(float(line.strip(' \t\n\r').split()[5]))
            decshift.append(float(line.strip(' \t\n\r').split()[6]))
            line_exp = line.split()[0].split('_')[0].split('.')[-1]
            if(abs(int(line_exp) - int(expnum)) < 7):
                ra_close.append(float(line.strip(' \t\n\r').split()[5]))
                dec_close.append(float(line.strip(' \t\n\r').split()[6]))

    if((len(rashift) == 0) or (len(decshift) == 0)):
        return [0, 0]
    if((len(ra_close) != 0) or (len(dec_close) != 0)):
        return[sum(ra_close) / len(ra_close), sum(dec_close) / len(dec_close)]
    else:
        return[sum(rashift) / len(rashift), sum(decshift) / len(decshift)]


def get_shift_exp(ut, ccd, exp, Field):
    pipeview_out = subprocess.check_output(['pipeview.pl',
                                            '-red',
                                            ut,
                                            '1-60',
                                            '-stage',
                                            'WCSNON',
                                            '-wcs',
                                            '-id',
                                            exp
                                            ],
                                           stderr=subprocess.STDOUT,
                                           universal_newlines=True
                                           )

    pipeview_out = pipeview_out.splitlines()[1:]

    rashift = []
    decshift = []

    ra_close = []
    dec_close = []

    for line in pipeview_out:
        # Checks Valid Line
        if((line.split()[0].split('.')[0] == Field) and
                (line.split()[1] == '1')):

            rashift.append(float(line.strip(' \t\n\r').split()[5]))
            decshift.append(float(line.strip(' \t\n\r').split()[6]))

            line_ccd = line.split()[0].split('_')[1]
            if(abs(int(line_ccd) - int(ccd)) < 3):
                ra_close.append(float(line.strip(' \t\n\r').split()[5]))
                dec_close.append(float(line.strip(' \t\n\r').split()[6]))

    if((len(rashift) == 0) or (len(decshift) == 0)):
        return [0, 0]
    if((len(ra_close) != 0) or (len(dec_close) != 0)):
        return[sum(ra_close) / len(ra_close), sum(dec_close) / len(dec_close)]
    else:
        return[sum(rashift) / len(rashift), sum(decshift) / len(decshift)]


def get_shift_field(ut, ccd, exp, Field):
    pipeview_out = subprocess.check_output(['pipeview.pl',
                                            '-red',
                                            ut,
                                            '1-60',
                                            '-stage',
                                            'WCSNON',
                                            '-wcs',
                                            '-im',
                                            Field],
                                           stderr=subprocess.STDOUT,
                                           universal_newlines=True
                                           )

    pipeview_out = pipeview_out.splitlines()[2:]

    rashift = []
    decshift = []

    ra_close = []
    dec_close = []

    for line in pipeview_out:
        # Checks Valid Line
        if((line.split()[0].split('.')[0] == Field) and
                (line.split()[1] == '1')):

            rashift.append(float(line.strip(' \t\n\r').split()[5]))
            decshift.append(float(line.strip(' \t\n\r').split()[6]))

            line_ccd = line.split()[0].split('_')[1]
            if(abs(int(line_ccd) - int(ccd)) < 3):
                ra_close.append(float(line.strip(' \t\n\r').split()[5]))
                dec_close.append(float(line.strip(' \t\n\r').split()[6]))

    if((len(rashift) == 0) or (len(decshift) == 0)):
        return [0, 0]
    if((len(ra_close) != 0) or (len(dec_close) != 0)):
        return[sum(ra_close) / len(ra_close), sum(dec_close) / len(dec_close)]
    else:
        return[sum(rashift) / len(rashift), sum(decshift) / len(decshift)]


def parse_args():
    default_photepipe = '/fred/oz100/pipes/arest/DECAM/DEFAULT/rawdata/'
    default_pushdir = '/fred/oz100/pipes/DWF_PIPE/CTIO_PUSH/'
    default_localdir = '/fred/oz100/pipes/DWF_PIPE/CTIO_PUSH/untar/'
    default_scamp = '/home/fstars/scamp_gaia/bin/scamp'
    default_gaia_dir = '/fred/oz100/pipes/DWF_PIPE/GAIA_DR2/'

    parser = argparse.ArgumentParser()
    parser.add_argument('-p',
                        '--push-dir',
                        metavar='DIRECTORY',
                        type=str,
                        default=default_pushdir,
                        help='Directory where tarballs of compressed '
                             'files are placed'
                        )

    parser.add_argument('-l',
                        '--local',
                        metavar='DIRECTORY',
                        type=bool,
                        default=True,
                        help='Use node local storage for jpg '
                             'to fits conversion?'
                        )

    parser.add_argument('--local-dir',
                        metavar='DIRECTORY',
                        type=str,
                        default=default_localdir,
                        help='Use node local storage for jpg '
                             'to fits conversion? (1 or 0)'
                        )

    parser.add_argument('--photepipe-rawdir',
                        metavar='DIRECTORY',
                        type=str,
                        default=default_photepipe,
                        help=''
                        )

    parser.add_argument('-i',
                        '--input-file',
                        type=str,
                        help='input .jp2 file'
                        )

    parser.add_argument('-d',
                        '--input-date',
                        type=str,
                        help='date for files during run'
                        )

    parser.add_argument('--debug',
                        action="store_true",
                        help='Turn on debug output.'
                        )

    parser.add_argument('--quiet',
                        action="store_true",
                        help='Turn off all non-essential debug output'
                        )

    parser.add_argument('--scamp-path',
                        type=str,
                        default=default_scamp,
                        help='Path to scamp',
                        )
    parser.add_argument('--gaia-dir',
                        metavar='DIRECTORY',
                        type=str,
                        default=default_gaia_dir,
                        help='Directory with Gaia data'
                        )

    args = parser.parse_args()

    return args


def main():
    start = datetime.datetime.now()

    args = parse_args()

    logfile = "prepipe_process_ccd_{}.log".format(
        start.strftime("%Y%m%d_%H:%M:%S")
    )

    logger = get_logger(args.debug, args.quiet, logfile=logfile)

    args = parse_args()

    # Set local Directory and check to see if it exists
    local_dir = Path(args.local_dir)
    local_convert = args.local

    if local_convert:
        if not local_dir.is_dir():
            logger.info(f'Creating Directory: {local_dir}')
            local_dir.mkdir()
    #local_dir = str(local_dir)

    photepipe_rawdir = Path(args.photepipe_rawdir)
    if photepipe_rawdir.stem != 'rawdata':
        logger.critical(f"Photepipe raw data directory should end with "
                        f"'rawdata'. Is instead {photepipe_rawdir}."
                        )
        exit()
    if not photepipe_rawdir.is_dir():
        logger.info(f'Creating Directory: {photepipe_rawdir}')
        photepipe_rawdir.mkdir()

    photepipe_workspace = Path(args.photepipe_rawdir.replace('rawdata',
                                                             'workspace')
                                                             )
    if not photepipe_workspace.is_dir():
        logger.info(f'Creating Directory: {photepipe_workspace}')
        photepipe_workspace.mkdir()

    push_dir = Path(args.push_dir)
    untar_path = push_dir / 'untar'

    file_name = args.input_file
    DECam_Root = file_name.split('.')[0]
    ccd_num = DECam_Root.split('_')[2]

    if(local_convert):
        # Move .jp2 to local directory
        logger.info(
            'Moving {untar_path / file_name} to {local_dir / file_name'
        )
        shutil.move(untar_path / file_name, local_dir / file_name)
        untar_path = local_dir

    # Uncompress Fits on local Directory
    uncompressed_fits = untar_path / DECam_Root + '.fits'
    logger.info('--------*****')
    logger.info(uncompressed_fits)
    logger.info('Uncompressing: ' + file_name + ' in path: ' + untar_path)
    logger.info('--------*****')
    uncompress_call = ['j2f_DECam',
                       '-i',
                       untar_path / file_name,
                       '-o',
                       uncompressed_fits,
                       '-num_threads',
                       str(1)
                       ]
    logger.info(f'Running {" ".join(uncompress_call)}')
    subprocess.run(uncompress_call)

    # Extract nescessary information from file for naming scheme
    exp = pyfits.getval(uncompressed_fits, "EXPNUM")
    Field = pyfits.getval(uncompressed_fits, "OBJECT")
    Filter = pyfits.getval(uncompressed_fits, "FILTER")[0]

    # FOR Chile!
    # FIX THIS.  So the problem is in a night's observations can straddle two
    # different ut's. The initial fits works but doesn't straddle month's,
    # plus since the check is based off of CURRENT time NOT observed time it
    # can screw up on reprocessing data.
    # Fix both of these.
    timestamp = datetime.datetime.utcnow().time()
    ut = args.input_date
    # if(timestamp > datetime.time(22,30)):
    # ut='ut'+str(int(pyfits.getval(uncompressed_fits,"OBSID")[6:12])+1)
    # else:
    # ut='ut'+pyfits.getval(uncompressed_fits,"OBSID")[6:12]

    obstype = pyfits.getval(uncompressed_fits, "OBSTYPE")

    newname = f"{Field}.{Filter}.{ut}.{exp}_{ccd_num}.fits"
    if((obstype == 'dome flat') or (obstype == 'domeflat')):
        newname = f"domeflat.{Filter}.{ut}.{exp}_{ccd_num}.fits"
    if((obstype == 'zero') or (obstype == 'bias')):
        newname = f"bias.{ut}.{exp}_{ccd_num}.fits"

    ut_dir = photepipe_rawdir / ut
    workspace_ut_dir = photepipe_workspace
    
    #if not ut_dir.is_dir():
    #    ut_dir.mkdir()
    
    dest_dir = ut_dir / ccd_num
    workspace_dest_dir = workspace_ut_dir / ccd_num
    if not dest_dir.is_dir():
        logger.info(f'Creating Directory: {dest_dir}')
        dest_dir.mkdir(parents=True)
    if not workspace_dest_dir.is_dir():
        logger.info(f'Creating Directory: {workspace_dest_dir}')
        workspace_dest_dir.mkdir(parents=True)

    # Move Uncompressed Fits File
    logger.info(f'Moving {uncompressed_fits} to {dest_dir / newname}')
    shutil.move(uncompressed_fits, dest_dir + newname)

    # Check for and prepare the calibration file lists
    flats_glob_str = str(dest_dir / f"domeflat.{Filter}.master.*")
    checkflats = glob.glob(flats_glob_str)

    if checkflats == []:
        logger.critical(
            "Prepipe Warning: No master flat detected! "
            "Looking for an individual flat.."
        )
        checkflats = glob.glob(str(dest_dir / f"domeflat.{Filter}*"))
        if checkflats == []:
            raise Exception("Prepipe Error: No flats detected! Exiting...")
    # Use the first in the list
    flat = checkflats[0]

    checkbias = glob.glob(str(dest_dir /"bias.master.*"))
    if checkbias == []:
        logger.info(
            "Prepipe Warning: No master bias detected! "
            "Looking for an individual bias.."
        )
        checkbias = glob.glob(str(dest_dir / "bias.*"))
        if checkbias == []:
            raise Exception("Prepipe Error: No bias detected! Exiting...")
    # Uuse the first in the list
    bias = checkbias[0]

    # Copy the raw image to the workspace, so that all the products
    # will be generated there.
    input_frames = workspace_dest_dir / newname

    subprocess.check_call(['cp',
                           dest_dir / newname,
                           input_frames
                           ]
                          )

    # Call Danny's preprocess code for CCD reduction
    preprocess_path = importlib.resources.path("dwfprepipe.bin",
                                               "prepipe_preprocess.py"
                                               )
    
    man_gaia = args.gaia_dir / f'{Field}_gaia_dr2_LDAC.fits'
    if not man_gaia.is_file():
        raise Exception(f"--man-gaia path ({man_gaia}) does not exist!")

    subprocess.check_call(["python",
                           f"{preprocess_path}",
                           f"--input-frames={input_frames}",
                           f"--flat-frames={flat}",
                           f"--bias-frames={bias}",
                           f"--with-scamp-exec={args.scamp_path}",
                           f"--man-gaia={man_gaia}"
                           ])

    # Remove unescessary .jp2
    logger.info('Deleting: ' + untar_path + file_name)
    subprocess.run(['rm', untar_path + file_name])


if __name__ == '__main__':
    main()
