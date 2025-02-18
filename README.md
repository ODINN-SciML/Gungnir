[![Build Status](https://github.com/ODINN-SciML/Gungnir/actions/workflows/CI.yml/badge.svg?branch=main)](https://github.com/ODINN-SciML/Gungnir/actions/workflows/CI.yml?query=branch%3Amain)

<img src="https://github.com/ODINN-SciML/Gungnir/blob/main/data/gungnir_logo.png" width="250">

Preprocessing of topographical and climate data for [ODINN.jl](https://github.com/ODINN-SciML/ODINN.jl) using [OGGM](https://github.com/OGGM/oggm).

Gungnir uses OGGM to generate all necessary files for the initial state and climate forcings to run simulations with ODINN.jl. Before running any simulations for specific glaciers with ODINN.jl, Gungnir needs to initialize those glaciers. We will progressively initialize glacier regions and store them in a server so they are readily available to all users. If you find that some glaciers or a region is missing, please contact us!

## Installation

All the notebooks inside this notebook can be executed after properly setting the environment. The `environment.yml` file can be used to
install all the required dependencies. Beside some standard Python dependencies, the `environment.yml` file include the installation of the module `gungnir` (included in this repository). The package `gungnir` includes all the code required to download the glacier data using OGGM.

In order to install the environment, you can use conda or mamba (see [Managing Environments](https://conda.io/projects/conda/en/latest/user-guide/tasks/manage-environments.html) for more information) with `conda env create -f environment.yml`. Once the environment is created, you can create the associated iPython kernel with 
```
python -m ipykernel install --user --name oggm_env_gungnir --display-name "IPython - Gungnir"
```
This will allow you to execute this environment directly from Jupyter notebooks. 

Alternatively, we included a `Makefile` that creates the conda environment and installs the associated iPython kernel so this environment can be accessible though Jupyter notebooks all at once. In order to use the Makefile, you need to open a terminal where the repository is located and enter
```
make env
```

Alternatively, if you just want to install the `gungnir` module, you can clone this repository and do
```
pip install gungnir
```
or
```
pip install -e gungnir
```
if you are working in developer mode. 

## Usage 

We included an example notebook of how to retrieve data using OGGM data in `notebooks/Example.ipynb`

You can also use Gungnir directly from the terminal. If you are using the remote OGGM cluster as working directory, in a new terminal after doing `conda activate oggm_env_gungnir`, proceed with
```bash
python gungnir/gungnir/preprocessing.py glaciers.txt
```
This will retrieve all the data of the glaciers included in `glaciers.txt`. If instead you are working in a local directory, simply do 
```bash
python gungnir/gungnir/preprocessing.py glaciers.txt <working-dir>
```