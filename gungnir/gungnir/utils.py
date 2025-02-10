
def read_glaciers_names(file):

    glaciers = []
        
    with open(file, "r") as f:
        for line in f:
            line = line.split(";")
            glaciers.append(line[0])

    return glaciers