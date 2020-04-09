#!/bin/bash 
#SBATCH --output=compress.out 
#SBATCH --error=compress.err 
#SBATCH --time=00:05:00 
#SBATCH --reservation=dwf 
source .bashrc 
python dwf_prepipe_processccd.py -i DECam_00912854_38.fits 
wait
python dwf_prepipe_processccd.py -i DECam_00912854_42.fits 
wait
python dwf_prepipe_processccd.py -i DECam_00912854_20.fits 
wait
python dwf_prepipe_processccd.py -i DECam_00912854_46.fits 
wait
python dwf_prepipe_processccd.py -i DECam_00912854_28.fits 
wait
python dwf_prepipe_processccd.py -i DECam_00912854_52.fits 
wait
python dwf_prepipe_processccd.py -i DECam_00912854_3.fits 
wait
python dwf_prepipe_processccd.py -i DECam_00912854_2.fits 
wait
python dwf_prepipe_processccd.py -i DECam_00912854_19.jp2 
wait
python dwf_prepipe_processccd.py -i DECam_00912854_21.fits 
wait
python dwf_prepipe_processccd.py -i DECam_00912854_34.jp2 
wait
python dwf_prepipe_processccd.py -i DECam_00912854_48.jp2 
wait
python dwf_prepipe_processccd.py -i DECam_00912854_33.jp2 
wait
python dwf_prepipe_processccd.py -i DECam_00912854_41.jp2 
wait
python dwf_prepipe_processccd.py -i DECam_00912854_46.jp2 
wait
python dwf_prepipe_processccd.py -i DECam_00912854_33.fits 
wait
python dwf_prepipe_processccd.py -i DECam_00912854_55.fits 
wait
python dwf_prepipe_processccd.py -i DECam_00912854_30.jp2 
wait
python dwf_prepipe_processccd.py -i DECam_00912854_37.jp2 
wait
python dwf_prepipe_processccd.py -i DECam_00912854_39.jp2 
wait
python dwf_prepipe_processccd.py -i DECam_00912854_45.jp2 
wait
python dwf_prepipe_processccd.py -i DECam_00912854_37.fits 
wait
python dwf_prepipe_processccd.py -i DECam_00912854_42.jp2 
wait
python dwf_prepipe_processccd.py -i DECam_00912854_53.jp2 
wait
python dwf_prepipe_processccd.py -i DECam_00912854_4.jp2 
wait
python dwf_prepipe_processccd.py -i DECam_00912854_3.jp2 
wait
python dwf_prepipe_processccd.py -i DECam_00912854_28.jp2 
wait
python dwf_prepipe_processccd.py -i DECam_00912854_54.jp2 
wait
python dwf_prepipe_processccd.py -i DECam_00912854_26.jp2 
wait
python dwf_prepipe_processccd.py -i DECam_00912854_21.jp2 
wait
python dwf_prepipe_processccd.py -i DECam_00912854_14.jp2 
wait
python dwf_prepipe_processccd.py -i DECam_00912854_45.fits 
wait
python dwf_prepipe_processccd.py -i DECam_00912854_13.jp2 
wait
python dwf_prepipe_processccd.py -i DECam_00912854_43.jp2 
wait
python dwf_prepipe_processccd.py -i DECam_00912854_38.jp2 
wait
python dwf_prepipe_processccd.py -i DECam_00912854_44.jp2 
wait
python dwf_prepipe_processccd.py -i DECam_00912854_36.jp2 
wait
python dwf_prepipe_processccd.py -i DECam_00912854_31.jp2 
wait
python dwf_prepipe_processccd.py -i DECam_00912854_44.fits 
wait
python dwf_prepipe_processccd.py -i DECam_00912854_26.fits 
wait
python dwf_prepipe_processccd.py -i DECam_00912854_12.jp2 
wait
python dwf_prepipe_processccd.py -i DECam_00912854_15.jp2 
wait
python dwf_prepipe_processccd.py -i DECam_00912854_54.fits 
wait
python dwf_prepipe_processccd.py -i DECam_00912854_20.jp2 
wait
python dwf_prepipe_processccd.py -i DECam_00912854_27.jp2 
wait
python dwf_prepipe_processccd.py -i DECam_00912854_2.jp2 
wait
python dwf_prepipe_processccd.py -i DECam_00912854_29.jp2 
wait
python dwf_prepipe_processccd.py -i DECam_00912854_55.jp2 
wait
python dwf_prepipe_processccd.py -i DECam_00912854_52.jp2 
wait
python dwf_prepipe_processccd.py -i DECam_00912854_5.jp2 
wait
