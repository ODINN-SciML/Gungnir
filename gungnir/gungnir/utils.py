import shutil
import os

def read_glacier_names(file):
    glaciers = []

    with open(file, "r") as f:
        for line in f:
            line = line.split(";")
            if len(line)==2:
                rgiid = line[0].split('#')[0].replace(' ', '') # Handle commented lines
                if rgiid!='': glaciers.append(rgiid)

    return glaciers

def emptyDir(folder):
    if os.path.isdir(folder): shutil.rmtree(folder)
    os.makedirs(folder)
