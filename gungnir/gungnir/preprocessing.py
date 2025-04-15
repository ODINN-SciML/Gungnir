import sys
from oggm import cfg, utils, workflow, tasks, global_tasks
from oggm.shop import bedtopo, millan22, glathida
from MBsandbox.mbmod_daily_oneflowline import process_w5e5_data
import json, os
from gungnir.utils import read_glacier_names, remove_id_from_string

_default_working_dir = utils.gettempdir('ODINN_prepro')

def preprocessing_file(file, working_dir=_default_working_dir):
    """
    Preprocess glaciers directly from file
    """

    rgi_ids = read_glacier_names(file)
    preprocessing_glaciers(rgi_ids, working_dir=working_dir)

def preprocessing_glaciers(rgi_ids, working_dir=_default_working_dir):
    """
    Preprocessing of glaciers from a list of glaciers

    Arguments:
        - rgi_ids: List of glaciers and/or regions to process. E.g., rgi_ids = ['RGI60-11.00897', 'RGI60-11.01270']
    """

    base_url = 'https://cluster.klima.uni-bremen.de/~oggm/gdirs/oggm_v1.6/L1-L2_files/elev_bands/'

    print("Working directory:", working_dir)

    cfg.initialize()

    # Settings
    cfg.PATHS['working_dir'] = working_dir
    cfg.PARAMS['use_multiprocessing'] = True # use all available CPUs
    cfg.PARAMS['border'] = 10
    cfg.PARAMS['hydro_month_nh'] = 1
    cfg.PARAMS['dl_verify'] = False
    cfg.PARAMS['continue_on_error'] = True

    # Now we initialize the glacier directories
    gdirs = workflow.init_glacier_directories(rgi_ids,
                                              prepro_base_url=base_url,
                                              from_prepro_level=2)

    # We execute the entity tasks
    list_tasks = [tasks.gridded_attributes,
                  tasks.glacier_masks,
                  bedtopo.add_consensus_thickness,
                  millan22.thickness_to_gdir,
                  millan22.velocity_to_gdir,
                  glathida.glathida_to_gdir]

    for task in list_tasks:
        workflow.execute_entity_task(task, gdirs)

    ### Then we retrieve all the necessary climate data ###
    rgi_paths = {}
    rgi_names = {}
    for gdir in gdirs: # TODO: change to parallel processing by creating an entity task
        # We store all the paths for each RGI ID to be retrieved later on in ODINN
        rgi_paths[gdir.rgi_id] = gdir.dir.replace(working_dir+'/', '')
        rgi_names[gdir.rgi_id] = remove_id_from_string(gdir.name)
        process_w5e5_data(gdir, climate_type='W5E5', temporal_resol='daily')

        print("dem path: " , gdir.get_filepath("dem"))

    with open(working_dir + '/rgi_paths.json', 'w') as f:
        json.dump(rgi_paths, f)
    with open(working_dir + '/rgi_names.json', 'w') as f:
        json.dump(rgi_names, f)

    # Verify that glaciers have no missing data
    task_log = global_tasks.compile_task_log(gdirs,
                                            task_names=["gridded_attributes", "velocity_to_gdir", "thickness_to_gdir"])

    task_log.to_csv(os.path.join(working_dir, "task_log.csv"))


if __name__ == "__main__":

    print(sys.argv)
    glacier_file = sys.argv[1]

    if len(sys.argv) == 2:
        preprocessing_file(glacier_file)
    else:
        working_dir = sys.argv[2]
        preprocessing_file(glacier_file, working_dir=working_dir)
