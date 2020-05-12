import subprocess

frame = 

# list a few astromatic config files
wd = os.path.dirname(__file__)
confdir = os.path.join(wd, 'config') 
scampconf = os.path.join(confdir, 'scamp.conf')
missfitsconf = os.path.join(confdir, 'missfits.conf')i
Field = "CDFS"
gaia_source = "/fred/oz100/pipes/DWF_PIPE/GAIA_DR2/{Field}_gaia_dr2_LDAC.fits"
catname = frame.replace('fits', 'cat')

syscall = 'scamp -c %s %s -ASTREF_CATALOG FILE -ASTREFCAT_NAME %s -ASTREFCENT_KEYS RA_ICRS,DE_ICRS -ASTREF    ERR_KEYS e_RA_ICRS,e_DE_ICRS -ASTREFMAG_KEY Gmag' % (scampconf, catname, gaia_source)i

print(syscall)
subprocess.check_call(syscall.split())

# fix the header
subprocess.check_call(["/home/fstars/missfits-2.8.0/bin/missfits",frame,frame.replace(".fits",".head"),f"-c {missfitsconf}"])
