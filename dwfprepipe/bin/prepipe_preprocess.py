#!/usr/bin/env python3
import os
import pdb
import subprocess
import argparse
import datetime
import importlib.resources

import numpy as np

from numpy import ma
from astropy.io import fits
from dwfprepipe.utils import get_logger

__whatami__ = 'Bias-correct, flat-field, astrometically calibrate, '\
              'and mask DECam images.'
__author__ = 'Danny Goldstein <danny@caltech.edu>'


def _read_clargs(val):
    if val[0].startswith('@'):
        # then its a list
        val = np.genfromtxt(val[0][1:], dtype=None, encoding='ascii')
        val = np.atleast_1d(val)
    return np.asarray(val)


def _parse_doubleslice(header, key, overscan=False):
    # get the good sections of the images
    goodsec = [tuple(map(int, s.split(':')))
               for s in header[key][1:-1].split(',')]
    slices = [slice(c[0] - 1, c[1]) for c in goodsec]
    if overscan:
        slices[0] = slice(goodsec[0][0] - 1 + 5, goodsec[0][1] - 5)
    return slices[::-1]


def overscan_and_mask_single(hdu):
    # Correct an image for overscan and create bad pixel masks based on
    # saturated pixels

    # create some data structures to store the output masks

    hdu.data = hdu.data.astype('float32')

    for amp in ['A', 'B']:
        over1, over2 = _parse_doubleslice(hdu.header,
                                          f'BIASSEC{amp}',
                                          overscan=True
                                          )
        data1, data2 = _parse_doubleslice(hdu.header, f'DATASEC{amp}')
        overbias = hdu.data[over1, over2].mean(axis=1)
        hdu.data[data1, data2] -= overbias[:, None]

    # do the saturation correction
    image = hdu.data
    mask = np.zeros_like(image, dtype='uint8')

    for amp in ['A', 'B']:
        saturval = hdu.header[f'SATURAT{amp}']
        amp1, amp2 = _parse_doubleslice(hdu.header, f'CCDSEC{amp}')
        mask[amp1, amp2][image[amp1, amp2] >= saturval] += 2 ** 0

    # create an HDUList object for the masks
    maskhdu = fits.ImageHDU(data=mask, header=hdu.header)

    return hdu, maskhdu


# recursively partition an iterable into subgroups
def _split(iterable, n):
    return [iterable[:len(iterable) // n]] + \
        _split(iterable[len(iterable) // n:], n - 1) if n != 0 else []


def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument('--flat-frames',
                        required=True,
                        help='Frames to use for flat fielding, in the same '
                        'order as --input-frames. If the argument is '
                        'prepended with "@", then it will be treated as a '
                        'list containing the names of the flats, '
                        'one per line.',
                        dest='flats',
                        nargs='+'
                        )

    parser.add_argument('--bias-frames',
                        required=True,
                        help='Frames to use for bias subtraction, in the same '
                        'order as --input-frames. If the argument is '
                        'prepended with "@", then it will be treated as a '
                        'list containing the names of the biases, '
                        'one per line.',
                        dest='biases',
                        nargs='+'
                        )

    parser.add_argument('--input-frames',
                        required=True,
                        help='Raw input frames. If the argument is prepended '
                             'with "@", then it will be treated as a list '
                             'containing the names of the input frames, '
                             'one per line.',
                        dest='frames',
                        nargs='+'
                        )

    parser.add_argument('--with-scamp-exec',
                        required=False,
                        default=None,
                        help='Use this executable for scamp instead '
                             'of the system default.',
                        dest='scampbin',
                        nargs=1
                        )

    parser.add_argument('--man-gaia',
                        required=False,
                        default=False,
                        help='manual imput of gaia catalog.',
                        dest='gaia_source'
                        )

    parser.add_argument('--use-mpi',
                        required=False,
                        default=False,
                        help='Parallelize with MPI.',
                        dest='mpi',
                        action='store_true'
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


def main():
    """
    Run script
    """

    start = datetime.datetime.now()

    args = parse_args()

    logfile = "prepipe_process_ccd_{}.log".format(
        start.strftime("%Y%m%d_%H:%M:%S")
    )

    logger = get_logger(args.debug, args.quiet, logfile=logfile)

    missfits_path = os.getenv("MISSFITS")
    if missfits_path is None:
        raise Exception("Path to MISSFITS is not specified")


    logger.debug("Running with arguments:")
    for arg, value in sorted(vars(args).items()):
        logger.debug(f"{arg}: {value}")

    if args.mpi:
        from mpi4py import MPI

        # set up mpi
        comm = MPI.COMM_WORLD
        rank = comm.Get_rank()
        size = comm.Get_size()

    # read the argparse input
    flats = _read_clargs(args.flats)
    biases = _read_clargs(args.biases)
    frames = _read_clargs(args.frames)
    gaia_source = _read_clargs(args.gaia_source)

    # distribute the work to the worker processes

    if args.mpi:
        ind = np.arange(len(flats))
        myind = _split(ind, size)[rank]

        myflats = flats[myind]
        mybiases = biases[myind]
        myframes = frames[myind]

    else:
        myflats = flats
        mybiases = biases
        myframes = frames

    # list a few astromatic config files
    with importlib.resources.path(
        "dwfprepipe.data.config", "scamp.sex"
    ) as path:
        sexconf = path

    with importlib.resources.path(
        "dwfprepipe.data.config", "default.nnw"
    ) as path:
        nnwname = path

    with importlib.resources.path(
        "dwfprepipe.data.config", "default.conv"
    ) as path:
        filtname = path

    with importlib.resources.path(
        "dwfprepipe.data.config", "scamp.param"
    ) as path:
        paramname = path

    with importlib.resources.path(
        "dwfprepipe.data.config", "scamp.conf"
    ) as path:
        scampconf = path

    with importlib.resources.path(
        "dwfprepipe.data.config", "missfits.conf"
    ) as path:
        missfitsconf = path

    # pass these constant options to sextractor
    clargs = ' -PARAMETERS_NAME %s -FILTER_NAME %s -STARNNW_NAME %s' % (
        paramname, filtname, nnwname)

    for flat, bias, frame in zip(myflats, mybiases, myframes):
        # Overscan
        with fits.open(frame) as hdul:
            ihdu, mhdu = overscan_and_mask_single(hdul[0])
            ccdnum_header = hdul[0].header["CCDNUM"]

            bpm_file = f"DECam_Master_20140209v2_cd_{ccdnum_header:.0f}.fits"
            with importlib.resources.path(
                "dwfprepipe.data.bpm", bpm_file
            ) as path:
                bpm_name = path

        with \
            fits.open(flat) as fl, \
            fits.open(bias) as b, \
            fits.open(bpm_name) as bp:

            fhdu = fl[0]
            bhdu = b[0]
            bphdu = bp[0]
            TRIM1, TRIM2 = _parse_doubleslice(ihdu.header, 'DATASEC')

            # Overscan the bias
            bhdu, mbhdu = overscan_and_mask_single(bhdu)
            # do the bias correction
            ccdnum = '%02d' % ihdu.header['CCDNUM']

            with np.errstate(divide='ignore', invalid='ignore'):
                ihdu.data[TRIM1, TRIM2] -= bhdu.data[TRIM1, TRIM2]

            # do the flat fielding
            fl1, fl2 = _parse_doubleslice(fhdu.header, 'DATASEC')
            flfield = fhdu.data[fl1, fl2]

            with np.errstate(divide='ignore', invalid='ignore'):
                calibpix = ihdu.data[TRIM1, TRIM2] / \
                    (flfield / np.median(flfield))

            # mask any resulting pixels that are invalid from the previous
            # operation
            mhdu.data[TRIM1, TRIM2][~np.isfinite(calibpix)] += 2 ** 1

            # and mask any that are bad from the badcol mask
            mhdu.data[TRIM1, TRIM2][bphdu.data != 0] += 2 ** 2

            # get the final mask
            finalmask = mhdu.data[TRIM1, TRIM2]

            # change the values of the science image to 1e-30 where there is a
            # bad pixel
            index_bad = np.where(finalmask != 0)
            calibpix[index_bad] = np.nan

            # save the flat-fielded and bias-corrected image to a new fits
            # image
            sciname = frame
            mskname = frame.replace('.fits', f'.mask.fits').replace('.fz', '')

            # update the header
            delkwds = []
            for card in ihdu.header.cards:
                if 'SEC' in card.keyword and card.keyword != 'DETSEC':
                    delkwds.append(card.keyword)

            for kwd in delkwds:
                del ihdu.header[kwd]

            hdul = fits.PrimaryHDU(
                calibpix.astype('float32'),
                header=ihdu.header)
            hdul.writeto(sciname, overwrite=True)
            mskhdul = fits.PrimaryHDU(finalmask, header=ihdu.header)
            mskhdul.writeto(mskname, overwrite=True)

            # now prepare to run source extractor
            syscall = 'sex -c %s -CATALOG_NAME %s -CHECKIMAGE_NAME %s %s'
            catname = frame.replace('fits', 'cat')
            chkname = frame.replace('fits', 'noise.fits')
            syscall = syscall % (sexconf, catname, chkname, sciname)
            syscall += clargs
            logger.info(f"Running sextractor with {syscall}")

            # call it
            subprocess.check_call(syscall.split())
            logger.info(f'sextractor complete for {sciname}')

            # now run scamp
            syscall = (f'scamp -c {scampconf} {catname} -ASTREF_CATALOG FILE '
                       f'-ASTREFCAT_NAME {gaia_source} -ASTREFCENT_KEYS '
                       f'RA_ICRS,DE_ICRS -ASTREFERR_KEYS e_RA_ICRS,e_DE_ICRS '
                       f'-ASTREFMAG_KEY Gmag'
                       )
            if args.scampbin is not None:
                syscall = args.scampbin[0] + syscall[5:]
            logger.info(f"Running scamp with {syscall}")
            subprocess.check_call(syscall.split())

            # fix the header
            subprocess.check_call([missfits_path,
                                   frame,
                                   frame.replace(".fits", ".head"),
                                   f"-c {missfitsconf}"
                                   ]
                                  )

            logger.info(f'scamp complete for {sciname}')


if __name__ == '__main__':
    main()
