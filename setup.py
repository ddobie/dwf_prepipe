from setuptools import setup, find_packages

import dwfprepipe

setup(
    name='dwfprepipe',
    url='https://github.com/ddobie/dwf_prepipe/',
    author='Dougal Dobie',
    author_email='ddobie@swin.edu.au',
    packages=find_packages(),
    version=dwfprepipe.__version__,
    license='MIT',
    description='Python module for DWF processing',
    install_requires=[],
    scripts=[
        "dwfprepipe/bin/prepipe_preprocess.py",
        "dwfprepipe/bin/prepipe_process_ccd.py",
        "dwfprepipe/bin/prepipe_reprocess.py",
        "dwfprepipe/bin/run_prepipe.py",
        "dwfprepipe/bin/run_push.py",
    ],
    include_package_data=True
)
