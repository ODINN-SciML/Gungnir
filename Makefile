.ONESHELL:
SHELL = /bin/bash

## env :              Creates conda environment and iPython kernel based on environment.yml file
env :
	conda env create -f environment.yml 
	conda activate oggm_env_gungnir
	conda install ipykernel
	python -m ipykernel install --user --name oggm_env_gungnir --display-name "IPython - Gungnir"
    
    
.PHONY : help
help : Makefile
	@sed -n 's/^##//p' $<