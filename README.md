# DWF Prepipe

This repository holds the code of DWF prepipe, a (mostly) python module to process data from the Dark Energy Camera (DECam) as part of the Deeper Wider Faster program.

## Contributors
### Current

* Dougal Dobie
* Natasha Van Bemmel

### Former
* Igor Andreoni
* Danny Goldstein
* Sara Webb
* Jielai Zhang

## Installation Instructions
`dwf_prepipe` can be installed using pip directly from github:
```
pip install git+https://github.com/ddobie/dwf_prepipe.git@vX.Y.Z
```
where `X.Y.Z` is the version to be installed.

To install the development version of `dwf_prepipe` please install `poetry` and then run
```
git clone https://github.com/ddobie/dwf_prepipe.git
cd dwf_prepipe
poetry install
```

The package also relies on a number of environment variables that specify default values of arguments:
* CTIO computers
  * `DATA_DIR` is the directory on the CTIO computers where the compressed tarballs are placed, ready for transfer
  * `QS` is the compression ratio to use
* OzSTAR
  * `PUSH_DIR` is the directory on ozstar where the compressed tarballs are pushed to, ready for unpacking.
