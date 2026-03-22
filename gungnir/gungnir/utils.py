import shutil
import os
import re

def read_glacier_names(file):
    glaciers = []

    with open(file, "r") as f:
        for line in f:
            if line[0]=='#' or line=='': continue
            line = line.split(";")
            if len(line)==2:
                rgiid = line[0].split('#')[0].replace(' ', '') # Handle commented lines
                if rgiid!='': glaciers.append(rgiid)

    return glaciers

def emptyDir(folder):
    if os.path.isdir(folder): shutil.rmtree(folder)
    os.makedirs(folder)

def remove_id_from_string(input_string):
    # Define the regex pattern to match IDs starting with "Fr" or "It"
    pattern = r'\b(?:Fr|It)[A-Za-z0-9]+\b'
    # Substitute the matched pattern with an empty string
    result = re.sub(pattern, '', input_string)
    # Remove any extra spaces introduced and return the cleaned string
    return result.strip()
