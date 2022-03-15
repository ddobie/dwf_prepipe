# DWF Prepipe

This repository holds the code of DWF prepipe, a (mostly) python module to process data from the Dark Energy Camera (DECam) as part of the Deeper Wider Faster program.

## Contributors
### Current

* Dougal Dobie

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

## Deploying to shared/remote servers
1. Log in to the remote server and [generate a new ssh key](https://docs.github.com/en/authentication/connecting-to-github-with-ssh/generating-a-new-ssh-key-and-adding-it-to-the-ssh-agent#generating-a-new-ssh-key)
2. Edit the `~/.ssh/config` file to add a new server (e.g. github.com-dwf_prepipe) per instructions [here](https://docs.github.com/en/developers/overview/managing-deploy-keys#using-multiple-repositories-on-one-server)
3. Set up the deploy keys per instructions [here](https://docs.github.com/en/developers/overview/managing-deploy-keys#deploy-keys)
4. Build a new python3.8 conda environment, e.g. `conda create --name prepipe python=3.8`
5. If necessary, install `poetry` with `pip install poetry`
6. Clone the repository with `git clone git@github.com-dwf_prepipe:ddobie/dwf_prepipe.git`
7. Install the repository as described above.
