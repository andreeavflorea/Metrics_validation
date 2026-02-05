import os, subprocess
import json, glob
from pathlib import Path
import shutil, fnmatch, re
import argparse
from typing import Dict, List, Set, Tuple
import concurrent.futures


'''
To obtain the files for the "mtriage", "real_space_refine" and "emringer" metrics of a specific map,
you only need to run the following command in the terminal:

    "python3 filename.py -m method_to_calculate_metrics -n number_of_maps -nw number_of_workers -p paths_file"

* filename.py in this case is: maps_metrics_files_v2.py
* method_to_calculate_metrics must be chosen from: average_maps, locscale, locscale-, locspiral, dem1, emr, emr2 or cryoten
* number_of_maps: number of maps for which to calculate the metrics
* number_of_workers: number of workers used in the parallelization
* paths_file: file where all the paths to be used for the metrics calculation are listed
'''

# ============================
# ARGUMENT PARSER
# ============================
parser = argparse.ArgumentParser(description='Select from which maps you want to obtain the metrics')
parser.add_argument('-m', type=str, required= True,
                    choices = ['average_maps', 'locscale', 'locscale-', 'locspiral', 'dem1',
                              'emr', 'emr2', 'cryoten', 'all', 'other'],
                    help='Type of maps to process (average_maps, locscale, locscale-, locspiral, dem1, emr, emr2, cryoten, all, other)')

parser.add_argument('--refine', action='store_true',
                    help='If set, run real_space_refine first, then mtriage and emringer to calculate metrics')

parser.add_argument('-n', type=int, default=None,
                    help='Number of maps to process from the list (if not provided, process all)')

parser.add_argument('-nw', type=int, default=5,
                    help='Number of workers to use to calculate metrics (if not provided, use 5)')

parser.add_argument('-p', type=str, required=True,
                    help='Path where the archive of path files is located')

args = parser.parse_args()


def limit_maps(maps):
    """
        Limits the number of maps to the specified amount.

        Parameters
        ----------
        maps : List[Any]
            The list of maps to be limited.
        args.n : Optional[int], default=None
            The maximum number of maps to keep. If None, no limit is applied.

        Returns
        -------
        List[Any]
            The limited list of maps.
        """
    if args.n is not None and len(maps) > args.n:
        print(f'Selecting the first {args.n} maps out of a total of {len(maps)}.')
        return maps[:args.n]  # Limit the list to the specified number given by --n
    return maps


def read_routes(file_path):
    """
        Reads a file containing key-value pairs separated by ' = '
        and returns them as a dictionary.

        Parameters
        ----------
        file_path : str
            Path to the file containing the routes.

        Returns
        -------
        Dict[str, str]
            Dictionary with keys and values parsed from the file.
        """

    routes = {}
    with open(file_path, 'r') as file:
        for line in file:
            line = line.strip()
            if line and ' = ' in line:
                key, value = line.split(' = ', 1)
                routes[key] = value.strip("'\"")
    return routes



def find_files(root_directory: str, json_file_path: str) -> Dict[str, List[str]]:
    """
      Searches for specific map and metadata files inside subdirectories of a given root directory,
      based on test values defined in a JSON file.

      Parameters
      ----------
      root_directory : str
          Path to the root directory containing subdirectories to search in.
      json_file_path : str
          Path to the JSON file with the test values under the key "prueba".

      Returns
      -------
      Dict[str, List[str]]
          A dictionary containing lists of sorted file paths for each file category:
          - "average_maps"
          - "half1_map"
          - "half2_map"
          - "locscale"
          - "locscale-"
          - "locspiral"
          - "dem1"
          - "emr"
          - "emr2"
          - "cryoten"
          - "other"
          - "json_files"
          - "pdb_files"
      """

    with open(json_file_path, "r", encoding="utf-8") as json_file:
        loaded_data = json.load(json_file)
        # >>> USER: change 'test' here if you want to process a different split
        test_values: List[str] = loaded_data["prueba_locscale"]

    print(f"\nData read from {os.path.basename(json_file_path)}:")
    print(len(test_values))
    print(sorted(test_values))

    # Initialization of file categories
    file_categories: Dict[str, Set[str]] = {
        "average_maps": set(),
        "half1_map": set(),
        "half2_map": set(),
        "locscale": set(),
        "locscale-": set(),
        "locspiral": set(),
        "dem1": set(),
        "emr": set(),
        "emr2": set(),
        "cryoten": set(),
        "other": set(),
        "json_files": set(),
        "pdb_files": set(),
    }

    # Pattern dictionary to reduce repetition
    patterns = {
        "_full.mrc": "average_maps",
        "_half1.mrc": "half1_map",
        "_half2.mrc": "half2_map",
        "_locscale.mrc": "locscale",
        "_locscale-.mrc": "locscale-",
        "_locspiral.mrc": "locspiral",
        "_full_dem1.mrc": "dem1",
        "_full_emr.mrc": "emr",
        "_full_emr2.mrc": "emr2",
        "_full_cryoten.mrc": "cryoten",
        "_full_other.mrc": "other"
    }

    # Iterate through subdirectories
    for test_value in test_values:
        # Traverses the subdirectories in the root directory
        for subdir in os.listdir(root_directory):
            full_path = os.path.join(root_directory, subdir)
            if os.path.isdir(full_path):
                for root, _, files in os.walk(full_path):
                    for archive in files:
                        # Search for matches with defined patterns
                        for suffix, category in patterns.items():
                            if archive == test_value + suffix:
                                file_categories[category].add(os.path.join(root, archive))

                        # JSONs
                        if archive == test_value + ".json":
                            file_categories["json_files"].add(os.path.join(root, archive))

                        # PDBs
                        if archive.endswith(".pdb"):
                            file_categories["pdb_files"].add(os.path.join(root, archive))

    # Convert sets to sorted lists
    result = {k: sorted(list(v)) for k, v in file_categories.items()}

    # Logs
    for category, files in result.items():
        print(f"LENGTH OF {category.upper()} LIST: {len(files)}")
        print("----------------------------------------")

    return result



def execute_command(map_type: str, output_directory: str, folder: list) -> list:
    """
    Executes the necessary commands to generate or fetch specific map types.

    This function iterates over all average maps and checks if the requested map type
    already exists in the specified output directory. If the map does not exist, it runs
    the appropriate command to generate it, depending on the map type. Generated maps
    are added to the provided folder list.

    Parameters
    ----------
    map_type : str
        Type of map to process (e.g., 'dem1', 'emr', 'emr2', 'cryoten', 'locspiral', 'locscale-').
    output_directory : str
        Directory where the processed maps should be stored.
    folder : list
        List where paths to processed or existing maps will be appended.

    Returns
    -------
    list
        Updated folder list containing paths to the processed or existing maps.
    """
    # Map each type to the corresponding command generator
    # >>> USER: modify this according to your installation
    command_map = {
        'dem1': lambda avg, out: f'deepemhancer -i {avg} -o {out} --deepLearningModelPath /home/andreea/.local/share/deepEMhancerModels/production_checkpoints/production_checkpoints/',
        'emr': lambda avg, out: f'~/Software/EMReady/EMReady_v1.2/EMReady.sh {avg} {out}',
        'emr2': lambda avg, out: f'~/Software/EMReady2/EMReady_v2.3/EMReady2.sh {avg} {out}',
        'cryoten': lambda avg, out: f'python3 /home/andreea/Software/cryoTEN/cryoten/eval.py {avg} {out}',
        'locscale-': lambda avg,
                            out: f'python3 {os.path.join(code_py_path, "locscale_corrected.py")} -m {avg} -o {out} -p {args.p}',
        'locspiral': lambda avg,
                            out: f'python3 {os.path.join(code_py_path, "locspiral.py")} -t locspiral -m {avg} -o {out}',
        'other': lambda avg, out: f'~/path/to/executable {avg} {out}'
    }

    # >>> USER: modify this according to your installation
    env_map = {
        'dem1': 'deepEMhancer_env',
        'emr': 'emready_env',
        'emr2': 'emready2_env',
        'cryoten': 'cryoten_env',
        'locscale-': '/home/jvargas/miniconda3/envs/locscale', #locscale
        'locspiral': 'scipion3',
        'other': 'other'

    }

    average_maps_list = files['average_maps']
    #json_files_list = files['json_files']
    #pdb_files_list = files['pdb_files']

    folder = []

    for i, avg_map in enumerate(average_maps_list):
        avg_map_path = Path(avg_map)
        base_name = avg_map_path.stem
        emd_number = base_name.split('_')[0]
        extension = avg_map_path.suffix.lstrip('.')

        # Determine output path
        if map_type in ['locspiral', 'locscale-']:
            output_file = Path(output_directory) / f"{emd_number}_{map_type}.mrc"
        else:
            output_file = Path(output_directory) / f"{base_name}_{map_type}.{extension}"

        if output_file.exists():
            print(f"The map {emd_number} already exists in {os.path.basename(output_directory)}")
            folder.append(str(output_file))
            continue

        print(f"Processing {map_type} for {emd_number}...")

        # Build command
        #if map_type in ['locspiral']:
        #    with open(json_files_list[i], 'r') as file:
        #        data = json.load(file)
        #    pdb_name = data['pdbs'][0]
        #    pdb_path = Path(pdb_files_list[0]).parent / f"{pdb_name}.pdb"
        #    command = command_map[map_type](avg_map, output_file, pdb_path, args.sym)
        #else:
        #    command = command_map[map_type](avg_map, output_file)

        command = command_map[map_type](avg_map, output_file)

        # >>> USER: modify this according to your installation
        cwd = Path('/home/andreea/Software/cryoTEN/cryoten') if map_type == 'cryoten' else None
        env = env_map[map_type]

        print(f"Running command for {map_type}: {command}")
        # >>> USER: modify this according to your installation
        subprocess.run(
            f"bash -c 'source /home/andreea/anaconda3/bin/activate {env} && {command} && conda deactivate'",
            shell=True,
            cwd=cwd
        )

        folder.append(str(output_file))
        print('---------------------------------------------')

    return folder



def sort_pdbs(maps: List[str]) -> Tuple[List[str], List[float]]:
    """
    Sort PDB files and retrieve their resolutions to match the list of map files.

    Parameters
    ----------
    maps : list of str
        List of map file paths.
    #json_files : list of str
    #    List of JSON metadata file paths corresponding to the maps.
    #pdb_files : list of str
    #    List of PDB file paths corresponding to the maps.

    Returns
    -------
    pdb_file : list of str
        List of sorted PDB file paths corresponding to the input maps.
    pdb_resolutions : list of float
        List of resolutions extracted from the JSON files corresponding to the maps.
    """

    pdb_file = []
    pdb_resolution = []

    for i, map_path in enumerate(maps):
        map_emd_number = Path(map_path).stem.split('_')[0].split('-')[-1]

        # Match JSON to map
        json_path = Path(files['json_files'][i])
        json_emd_number = json_path.stem.split('-')[-1]
        json_path = Path(str(json_path).replace(json_emd_number, map_emd_number))

        with open(json_path, 'r') as f:
            data = json.load(f)

        pdb_resolution.append(data['resolution'])

        # Match PDB to map
        pdb_path = Path(files['pdb_files'][i]).with_name(f"{data['pdbs'][0]}.pdb")
        pdb_file.append(str(pdb_path))

    return pdb_file, pdb_resolution




def equal_length(map_type: str, maps_list: List[str]) -> Tuple[List[str], List[str], List[float]]:
    """
        Ensures all lists are aligned and limited in length.
        For methods that generate new maps, executes them first.

        Parameters
        ----------
        map_type : str
            Type of map (e.g., 'dem1', 'emr', 'locspiral', 'locscale-', 'locscale', 'average_maps', 'other').
        maps_list : List[str]
            List of maps to process.

        Returns
        -------
        Tuple[List[str], List[str], List[float]]
            - Limited maps
            - Sorted PDB files
            - Resolutions
    """

    print(f"\n--- Processing {map_type.upper()} maps ---")
    print(f"Initial number of AVERAGE maps: {len(files['average_maps'])}")
    print(f"Initial number of {map_type.upper()} maps: {len(maps_list)}")
    print('---------------------------------------------')

    generation_methods = {
        'dem1': output_directory_dem1,
        'emr': output_directory_emr,
        'emr2': output_directory_emr2,
        'cryoten': output_directory_cryoten,
        'locspiral': output_directory_locspiral,
        'locscale-': output_directory_locscale_,
        'other': output_directory_other
    }

    if map_type in generation_methods:
        maps_list = execute_command(map_type, generation_methods[map_type], maps_list)
        print(f"{map_type.upper()} maps generated/validated: {len(maps_list)}")
    else:
        print(f"{map_type.upper()} maps do not require generation (using existing files).")

    print('---------------------------------------------')
    pdb_file, pdb_resolution = sort_pdbs(maps_list)
    print(f"PDB files and resolutions extracted for {map_type}.")
    print('---------------------------------------------')

    # Apply optional limit (from CLI arg --n)
    maps = limit_maps(maps_list)
    pdb_file = pdb_file[:len(maps)]
    pdb_resolution = pdb_resolution[:len(maps)]

    print(
        f"Final lengths → maps: {len(maps)}, pdbs: {len(pdb_file)}, resolutions: {len(pdb_resolution)}")
    print(f"Selected maps {map_type.upper()}: ready for metric computation \n {maps}")
    print('---------------------------------------------')

    return maps, pdb_file, pdb_resolution



def run_qscore(map_path: str, pdb_path: str, pdb_resolution: float, log_file_name: str, output_directory: str, extra_directory: str, map_type: str, refined=True):
    """
        Runs UCSF Chimera's Q-score calculation for a given map–PDB pair and saves logs/results.

        Parameters
        ----------
        map_path : str
            Path to the .mrc map file.
        pdb_path : str
            Path to the corresponding .pdb file.
        pdb_resolution : float
            Resolution value for the PDB structure.
        log_file_name : str
            Base name used to create the Q-score log file.
        output_directory : str
            Directory where Q-score logs will be stored.
        extra_directory : str
            Directory for additional Q-score result files.
        map_type : str
            Map category (e.g., 'average_maps', 'locspiral', 'cryoten', etc.).
        refined : bool, optional
            Whether the map has been refined. Default is True.
        """

    suffix = f"_{map_type}" if map_type != "average_maps" else ""
    suffix += "" if refined else "_not_refine"
    log_output_qscore = os.path.join(output_directory, f'mapq.{log_file_name}_qscore_avg{suffix}.log')

    qscores_dir = os.path.join(extra_directory, 'qscores_extra')
    os.makedirs(qscores_dir, exist_ok=True)

    if os.path.exists(log_output_qscore):
        print("~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~")
        print(f"Skipping Q-score for {log_file_name}: already exists in {output_directory}")
        print("~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~")
        return

    print(f"\nExecuting Q-score for {map_path} and {pdb_path}...")
    # >>> USER: modify this according to your installation
    command_qscore = [
        'python3',
        '/home/andreea/.local/UCSF-Chimera64-1.17.3/share/mapq/mapq_cmd.py',
        '/home/andreea/.local/UCSF-Chimera64-1.17.3',
        f'map={map_path}',
        f'pdb={pdb_path}',
        f'res={pdb_resolution}',
        'np=2'
    ]
    print(f"Command: {command_qscore}\nOutput: {log_output_qscore}")

    with open(log_output_qscore, 'w') as f:
        subprocess.run(command_qscore, stdout=f, stderr=subprocess.STDOUT)

    pdb_base = os.path.basename(pdb_path)
    map_base = os.path.basename(map_path)

    qscore_files = [
        f"{pdb_base}__Q__{map_base}_All.txt",
        f"{pdb_base}__Q__{map_base}.pdb"
    ]

    pdb_dir = os.path.dirname(pdb_path)
    for file in qscore_files:
        try:
            shutil.move(os.path.join(pdb_dir, file), os.path.join(qscores_dir, file))
            print(f"{file} moved to {qscores_dir}")
        except FileNotFoundError:
            print(f"File {file} not found")


# ============================================================
# General command execution helper
# ============================================================
def run_command(command):
    """
        Executes a system command safely and prints feedback.

        Parameters
        ----------
        command : list or str
            The command to execute. Can be a list of arguments or a single string.

        Returns
        -------
        None
        """
    # Run the command
    try:
        subprocess.run(' '.join(command), shell=True, check=True)
        print(f'Command executed successfully: {" ".join(command)}')
    except subprocess.CalledProcessError as e:
        print(f'Error executing command: {" ".join(command)}')
        print(e)


def delete_file(output_file: str, metric: str) -> None:
    """
    Deletes a file that caused metric calculation failure and logs the reason.

    Parameters
    ----------
    output_file : str
        Path to the file that needs to be deleted.
    metric : str
        Name of the metric that could not be calculated for this map.

    Returns
    -------
    None
    """
    try:
        os.remove(output_file)
        print(
            f'The file {os.path.basename(output_file)} had to be deleted '
            f'because the {metric} could not be calculated for this map.')

    except OSError as error_os:
        print(f"Error while trying to delete {os.path.basename(output_file)}: {error_os}")



def run_mtriage_emringer(map_path: str, half1_map: str, half2_map: str, refined_pdb: str, pdb_file_name: str, log_file_name: str, map_type: str, refine_first=True):
    """
        Run Phenix mtriage and emringer metrics for a given map and PDB structure.

        Parameters
        ----------
        map_path : str
            Path to the processed or target map.
        half1_map : str
            Path to the first half-map (used only for 'average_maps').
        half2_map : str
            Path to the second half-map (used only for 'average_maps').
        refined_pdb : str
            Path to the refined PDB model.
        pdb_file_name : str
            Base filename of the PDB structure (used for naming outputs).
        log_file_name : str
            Name to include in log files.
        map_type : str
            Type of map being processed (e.g., 'average_maps', 'emr', 'cryoten', etc.).
        refine_first : bool, optional
            Whether the model was refined before metric calculation (default=True).
        """


    # Determine output suffixes
    suffix = f"_{map_type}" if map_type != "average_maps" else ""
    suffix += "_not_refine" if not refine_first else ""

    output_file_mtriage = os.path.join(output_directory, f'phenix.{log_file_name}_mtriage{suffix}.log')
    output_file_emringer = os.path.join(output_directory, f'phenix.{log_file_name}_emringer{suffix}.log')

    # ============================================================
    # PHENIX MTRIAGE
    # ============================================================
    if os.path.exists(output_file_mtriage):
        print("~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~")
        print(f'Skipping mtriage for {log_file_name}: log already exists in {output_directory}')
        print("~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~")

    else:
        # Build the command
        if map_type == 'average_maps':
            command_mtriage = ['phenix.mtriage',
                               map_path,
                               half1_map,
                               half2_map,
                               refined_pdb,
                               '>', output_file_mtriage]
        else:
            command_mtriage = ['phenix.mtriage',
                               map_path,
                               refined_pdb,
                               '>', output_file_mtriage]

        print('-----------------------------------------')
        print('-----------------------------------------')
        print('-----------------------------------------')
        print(f'MTRIAGE COMMAND: {" ".join(command_mtriage)}')
        print(f'Output: {output_file_mtriage}')
        run_command(command_mtriage)

    # ============================================================
    # PHENIX EMRINGER
    # ============================================================
    if os.path.exists(output_file_emringer):
        print("~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~")
        print(f'Skipping emringer for {log_file_name}: log already exists in {output_directory}')
        print("~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~")
        return

    command_emringer = ['phenix.emringer',
                        refined_pdb,
                        map_path,
                        '>', output_file_emringer]

    print('-----------------------------------------')
    print('-----------------------------------------')
    print('-----------------------------------------')
    print(f'EMRINGER COMMAND: {" ".join(command_emringer)}')
    print(f'Output: {output_file_emringer}')

    # Execute the command and capture output
    result_emringer = subprocess.run(" ".join(command_emringer), capture_output=True, shell=True)
    stderr_emringer = result_emringer.stderr.decode('utf-8') if result_emringer.stderr else ""

    if result_emringer.returncode != 0:
        print(f'Error running: {" ".join(command_emringer)}')
        print('~~~~~~~~~~~~~~~~~~~~~')
        print(f"EMRINGER ERROR DETAILS: {stderr_emringer}")
        print('~~~~~~~~~~~~~~~~~~~~~')

        if "ValueError: max() arg is an empty sequence" in stderr_emringer:
            print('~~~~~~~~ ERROR: Empty sequence in EMRINGER ~~~~~~~~')
            print('Cannot calculate EMRINGER metrics for this map.')
            print('~~~~~~~~~~~~~~~~~~~~~')
            delete_file(output_file_emringer, 'emringer')
            return

        elif "ValueError: residue.id_str(suppress_segid=false): segid is not unique" in stderr_emringer:
            print('~~~~~~~~ ERROR: Duplicate SEGID found in EMRINGER ~~~~~~~~')
            print('Removing problematic SEGIDs and retrying...')
            print('~~~~~~~~~~~~~~~~~~~~~')
            output_pdb_segid = os.path.join(code_py_path, f'{pdb_file_name}_segid.pdb')
            print(f"Output PDB with Segid correction {output_pdb_segid} ")

            remove_segid(refined_pdb, output_pdb_segid)

            command_emringer_segid = ['phenix.emringer',
                                      output_pdb_segid,
                                      map_path,
                                      '>', output_file_emringer]
            print('-----------------------------------------')
            print(f'EMRINGER SEGID COMMAND: {" ".join(command_emringer_segid)}')
            run_command(command_emringer_segid)

        else:
            print('Unknown EMRINGER error occurred.')
            delete_file(output_file_emringer, 'emringer')
            return

    # ============================================================
    # FILE MANAGEMENT
    # ============================================================
    manage_output_files(pdb_file_name=pdb_file_name, code_py_path=code_py_path,
                        extra_directory=extra_directory,  process_type="emringer")

    manage_output_files(pdb_file_name=pdb_file_name, code_py_path=code_py_path,
                        extra_directory=extra_directory, process_type="mtriage")


def run_real_space_refine(map_path: str, pdb_file: str, pdb_file_name: str, pdb_resolution: float, log_file_name: str, map_type: str):
    """
        Runs phenix.real_space_refine on a map and corresponding PDB file.
        Handles common errors by cleaning the PDB or running pdb_interpretation/ready_set.
        Moves all generated files to the extra directory.

        Parameters
        ----------
        map_path : str
            Path to the map to refine.
        pdb_file : str
            Path to the PDB file to refine.
        pdb_file_name : str
            Base name of the PDB file (without extension) used for naming outputs.
        pdb_resolution : float
            Resolution of the map.
        log_file_name : str
            Base name for log files.
        map_type : str
            Type of map (e.g., "average_maps", "emr", etc).

        Returns
        -------
        str | None
            Path to the refined PDB file, or None if refinement failed.
        """

    max_iterations = 5
    suffix = f"_{map_type}" if map_type != "average_maps" else ""
    output_file_refinement = os.path.join(output_directory, f"phenix.{log_file_name}_real_space_refined{suffix}.log")

    # Check if refinement was already done
    if os.path.exists(output_file_refinement):
        print("~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~")
        print(f"Skipping real_space_refine for {log_file_name}: log already exists in {output_directory}")
        cleaned_pdb_path = os.path.join(extra_directory, f"{pdb_file_name}_cleaned_real_space_refined_000.pdb")
        refined_pdb_path = os.path.join(extra_directory, f"{pdb_file_name}_real_space_refined_000.pdb")

        # This conditional sentences are applied when real_space_refine is executed first
        if os.path.exists(cleaned_pdb_path):
            refined_pdb = cleaned_pdb_path
            #return cleaned_pdb
        elif os.path.exists(refined_pdb_path):
            refined_pdb = refined_pdb_path
            #return refined_pdb
        else:
            refined_pdb = None
            print(f"[WARNING] No refined PDB found for {pdb_file_name} in {extra_directory}")

        print(f"Refined PDB {refined_pdb}")
        print("~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~")
        return refined_pdb


    iteration = 0
    refinement_success = True

    command_refinement = [
        'phenix.real_space_refine',
        pdb_file,
        map_path,
        f'resolution={pdb_resolution}',
        '>', output_file_refinement
    ]

    print('-----------------------------------------')
    print('-----------------------------------------')
    print('-----------------------------------------')
    print('REFINEMENT COMMAND:', command_refinement)
    # Output
    print('Output refinement', output_file_refinement)

    try:
        result = subprocess.run(" ".join(command_refinement), capture_output=True, shell=True, text=True)
        stderr = result.stderr

        if result.returncode != 0:
            print(f"Error running command: {' '.join(command_refinement)}")
            print('~~~~~~~~~~~~~~~~~~~~~')
            print(f"Specific error: {stderr}")
            print('~~~~~~~~~~~~~~~~~~~~~')

            # Handle fatal PDB interpretation errors
            if "Sorry: Fatal problems interpreting model file" in stderr:
                print("Fatal error interpreting PDB. Starting iterative PDB cleaning...")

                os.makedirs(extra_directory, exist_ok=True)
                while refinement_success and iteration < max_iterations:
                    print(f"\n==== Attempt {iteration + 1} ====")
                    pdb_interpret_log = os.path.join(extra_directory,
                                                     f'phenix.{log_file_name}_pdb_interpretation.log')
                    command_pdb_interpretation = ['phenix.pdb_interpretation',
                                                  pdb_file,
                                                  '>', pdb_interpret_log]
                    print("Pdb interpretation command:", command_pdb_interpretation)
                    print('-------------------------------------------')
                    print('-------------------------------------------')
                    run_command(command_pdb_interpretation)

                    # Parse residues to remove
                    with open(pdb_interpret_log, 'r') as file:
                        residues_to_remove = set()
                        #lines = file.readlines()
                        for line in file:
                            if "nonbonded pdb" in line:
                                residues_to_remove.add((line[22:26].strip(), line[26].strip(), line[27:32].strip()))

                                #residues = [(line[22:26].strip(), line[26].strip(), line[27:32].strip())]
                                #for residue in residues:
                                #    residues_to_remove.add(residue)

                    print(f"-----------------Residues to remove-----------------\n {residues_to_remove}")
                    print('-------------------------------------------')

                    # Clean PDB
                    with open(pdb_file, 'r') as pdb_in:
                        lines_in = pdb_in.readlines()

                    pdb_file_cleaned = os.path.join(extra_directory, f"{pdb_file_name}_cleaned.pdb")

                    with open(pdb_file_cleaned, 'w') as pdb_out:
                        for line_in in lines_in:
                            if line_in.startswith(('ATOM', 'HETATM')):
                                res_name = line_in[17:20].strip()
                                chain_id = line_in[21].strip()
                                res_num = line_in[22:26].strip()
                                if (res_name, chain_id, res_num) not in residues_to_remove:
                                    pdb_out.write(line_in)
                            else:
                                pdb_out.write(line_in)

                    # Update pdb_file to the cleaned PDB for the next iteration
                    pdb_file = pdb_file_cleaned

                    # Run refinement on cleaned PDB
                    command_cleaned_refine = [
                        'phenix.real_space_refine',
                        pdb_file_cleaned,
                        map_path,
                        f'resolution={pdb_resolution}',
                        '>', output_file_refinement
                    ]
                    print("REFINEMENT COMMAND (cleaned PDB):", command_cleaned_refine)
                    result_cleaned = subprocess.run(" ".join(command_cleaned_refine), capture_output=True,
                                                    shell=True, text=True)
                    stderr_cleaned = result_cleaned.stderr

                    if result_cleaned.returncode == 0:
                        print("~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~")
                        print("Refinement succeeded after cleaning PDB.")
                        print("~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~")
                        refinement_success = False
                    else:
                        print("~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~")
                        print(f"Error after cleaned refinement: {stderr_cleaned}")
                        print("~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~")
                        iteration += 1

                    #refinement_patterns = [
                    #    f"{pdb_file_name}_real_space_refined_000.pdb",
                    #    f"{pdb_file_name}_real_space_refined_000.log",
                    #    f"{pdb_file_name}_real_space_refined_000.eff",
                    #    f"{pdb_file_name}_real_space_refined_000.cif",
                    #    f"{pdb_file_name}_real_space_refined_000_initial.geo",
                    #    f"{pdb_file_name}_cleaned_real_space_refined_000.pdb",
                    #    f"{pdb_file_name}_cleaned_real_space_refined_000.log",
                    #    f"{pdb_file_name}_cleaned_real_space_refined_000.eff",
                    #    f"{pdb_file_name}_cleaned_real_space_refined_000.cif",
                    #    f"{pdb_file_name}_cleaned_real_space_refined_000_initial.geo"
                    #]

                    #for pattern in refinement_patterns:
                    #    for file_path in glob.glob(os.path.join(code_py_path, pattern)):
                    #        try:
                    #            shutil.move(file_path, os.path.join(extra_directory, os.path.basename(file_path)))
                    #            print(
                    #                f"Moved previous refinement output {os.path.basename(file_path)} → {extra_directory}")
                    #            if os.path.basename(file_path)  == f"{pdb_file_name}_cleaned_real_space_refined_000.pdb":
                    #                refined_pdb = os.path.join(extra_directory, os.path.basename(file_path) )
                    #        except Exception as e:
                    #            print(f"[WARNING] Could not move {file_path}: {e}")

                    refined_pdb = manage_output_files(pdb_file_name=pdb_file_name,
                                                      code_py_path=code_py_path,
                                                      extra_directory=extra_directory,
                                                      process_type="refine")


                if iteration > max_iterations:
                    print("~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~")
                    print(
                        f"The maximum number of iterations ({max_iterations}) has been reached. The refinement was not successful.")


            elif RuntimeError:
                print("There are geometry discrepancies in the model with excessively long bond lengths")

                # === STEP 1: Run phenix.ready_set ===
                output_file_ready = os.path.join(code_py_path, f'{pdb_file_name}.updated.pdb')
                command_ready = ['phenix.ready_set',
                                 pdb_file,
                                 '>', output_file_ready]

                print('READY_SET COMMAND:', ' '.join(command_ready))
                run_command(command_ready)

                # === STEP 2: Remove old log file that prevents further calculations ===
                log_file = os.path.join(code_py_path, f'{pdb_file_name}_real_space_refined_000.log')

                if os.path.exists(log_file):
                    print('~~~~~~~~~~~~~~~~~~~~~')
                    print(f'Removing old log file {os.path.basename(log_file)} '
                          f'because otherwise a new one will be generated that prevents further metric calculations')
                    os.remove(log_file)
                    print('~~~~~~~~~~~~~~~~~~~~~')

                # === STEP 3: Run new refinement ===
                command_refinement_new = [
                    'phenix.real_space_refine',
                    output_file_ready,      # pdb updated
                    map_path,
                    f'resolution={pdb_resolution}',
                    '>', output_file_refinement
                ]
                print('-------------- NEW REFINEMENT AFTER READY_SET -----------------')
                print('NEW REFINEMENT COMMAND:', ' '.join(command_refinement_new))
                print('---------------------------------------------------------------------')

                run_command(command_refinement_new)

                # === STEP 4: Automatically manage the resulting files ===
                print("Organizing results from READY_SET refinement...")

                refined_pdb = manage_output_files(
                    pdb_file_name=pdb_file_name,
                    code_py_path=code_py_path,
                    extra_directory=extra_directory,
                    process_type="refine"  # Only move refinement outputs
                )

                if refined_pdb:
                    print(f"Refinement successfully completed: {refined_pdb}")
                else:
                    print("Refined PDB file not found after ready_set.")


            # If other errors occur, report
            else:
                print("Other error during refinement.")
                refined_pdb = None


        else:
            print(f'Command executed successfully {" ".join(command_refinement)}')
            refined_pdb = manage_output_files(pdb_file_name=pdb_file_name,
                                              code_py_path=code_py_path,
                                              extra_directory=extra_directory,
                                              process_type="refine")

        print('REFINED PDB', refined_pdb)
        return refined_pdb


    except Exception as e:
        print('---------------------------------ERROR---------------------------------')
        print(f'Real space refinement could not be performed for this map {log_file_name}')
        print(f"Exception: {str(e)}")
        print('--------------------------------------------------------------------------------')
        return None



def manage_output_files(pdb_file_name: str, code_py_path: str, extra_directory: str, process_type: str = "all"):
    """
    Locate and move relevant Phenix output files (real-space refinement, Mtriage, or EMRinger)
    depending on the selected process type.

    Parameters
    ----------
    pdb_file_name : str
        The base name of the processed PDB file.
    code_py_path : str
        Directory where Phenix generates its output files.
    extra_directory : str
        Destination directory where results should be moved.
    process_type : str, optional
        Type of process to handle ("refine", "mtriage", "emringer", or "all").
        Defaults to "all".

    Returns
    -------
    str | None
        Path to the final refined PDB file (if applicable), or None.
    """
    # Define output folders
    #refinement_dir = os.path.join(extra_directory, "refinement_outputs")
    mtriage_dir = os.path.join(extra_directory, "mtriage_outputs")
    emringer_dir = os.path.join(extra_directory, "emringer_outputs")
    plots_dir = os.path.join(extra_directory, "emringer_plots_extra")

    for folder in [mtriage_dir, emringer_dir, plots_dir]:
        os.makedirs(folder, exist_ok=True)

    # === Define expected refinement output sets ===
    refinement_patterns = [
       f"{pdb_file_name}_real_space_refined_000.pdb",
       f"{pdb_file_name}_real_space_refined_000.log",
       f"{pdb_file_name}_real_space_refined_000.eff",
       f"{pdb_file_name}_real_space_refined_000.cif",
       f"{pdb_file_name}_real_space_refined_000_initial.geo",
       f"{pdb_file_name}_cleaned_real_space_refined_000.pdb",
       f"{pdb_file_name}_cleaned_real_space_refined_000.log",
       f"{pdb_file_name}_cleaned_real_space_refined_000.eff",
       f"{pdb_file_name}_cleaned_real_space_refined_000.cif",
       f"{pdb_file_name}_cleaned_real_space_refined_000_initial.geo",
       f"{pdb_file_name}.eff",
       f"{pdb_file_name}.updated.cif",
       f"{pdb_file_name}.updated.pdb"
    ]

    # === Define patterns for mtriage / emringer ===
    mtriage_patterns = [
        f"{pdb_file_name}*mtriage*.*",
        "fsc_*.xml",
        "fsc_*.log",
        "mask.ccp4"
    ]
    emringer_patterns = [
        f"{pdb_file_name}*emringer*.*"
    ]

    print('-----------------------------------------')
    print(f"Collecting all relevant Phenix output files for {pdb_file_name}...")
    print(f"Managing Phenix output files for {pdb_file_name} (process: {process_type})")


    refined_pdb = None
    # === Handle each process selectively ===
    if process_type in ("refine", "all"):
        print("→ Handling refinement outputs...")

        for pattern in refinement_patterns:
           for file_path in glob.glob(os.path.join(code_py_path, pattern)):
               try:
                   shutil.move(file_path, os.path.join(extra_directory, os.path.basename(file_path)))
                   print(
                       f"Moved previous refinement output {os.path.basename(file_path)} → {extra_directory}")
                   if os.path.basename(file_path) in (f"{pdb_file_name}_cleaned_real_space_refined_000.pdb"
                                                      f"{pdb_file_name}_real_space_refined_000.pdb"):
                       refined_pdb = os.path.join(extra_directory, os.path.basename(file_path))
               except Exception as e:
                   print(f"[WARNING] Could not move {file_path}: {e}")
        print('-----------------------------------------')
        if refined_pdb:
            print(f"Final refined PDB located: {refined_pdb}")
        else:
            print(f"No final refined PDB found for {pdb_file_name}")

    # === Handle mtriage outputs ===
    elif process_type in ("mtriage", "all"):
        print("→ Handling Mtriage outputs...")
        for pattern in mtriage_patterns:
            for file_path in glob.glob(os.path.join(code_py_path, pattern)):
                file_name = os.path.basename(file_path)
                try:
                    shutil.move(file_path, os.path.join(mtriage_dir, file_name))
                    print(f"Moved {file_name} → {mtriage_dir}")
                except Exception as e:
                    print(f"Error moving {file_name}: {e}")

    # === Handle emringer outputs ===
    elif process_type in ("emringer", "all"):
        print("→ Handling EMRinger outputs...")
        for pattern in emringer_patterns:
            for file_path in glob.glob(os.path.join(code_py_path, pattern)):
                file_name = os.path.basename(file_path)
                try:
                    shutil.move(file_path, os.path.join(emringer_dir, file_name))
                    print(f"Moved {file_name} → {emringer_dir}")
                except Exception as e:
                    print(f"Error moving {file_name}: {e}")

        # === Handle EMRinger plot folders ===
        for folder in os.listdir(code_py_path):
            if fnmatch.fnmatch(folder, "*emringer_plots"):
                src_path = os.path.join(code_py_path, folder)
                dst_path = os.path.join(plots_dir, folder)
                if os.path.exists(dst_path):
                    shutil.rmtree(dst_path)
                shutil.move(src_path, dst_path)
                print(f"Moved EMRinger plot folder: {folder}")




    return refined_pdb



def remove_segid(input_pdb: str, output_pdb: str):
    """
    Remove SEGID identifiers from ATOM and HETATM records in a PDB file.

    This function processes a PDB file line by line, erasing the SEGID field (columns 73–76)
    that can cause issues in programs like Phenix during metric calculations such as EMRinger.
    A new cleaned version of the PDB file is written to the specified output path.

    Parameters
    ----------
    input_pdb : str
        Path to the original PDB file containing potential SEGID conflicts.
    output_pdb : str
        Path where the cleaned PDB file without SEGID fields will be saved.

    Notes
    -----
    - The SEGID field occupies columns 73–76 of a standard PDB line format.
    - Only lines starting with 'ATOM' or 'HETATM' are modified; all others are copied unchanged.
    - Useful for resolving errors such as:
      ``ValueError: residue.id_str(suppress_segid=false): segid is not unique``
    """

    with open(input_pdb, 'r') as file:
        lines = file.readlines()

    with open(output_pdb, 'w') as file:
        for line in lines:
            if line.startswith(('ATOM', 'HETATM')):
                line = line[:72] + ' ' * 4 + line[76:]
            file.write(line)


def metrics(map_type: str, maps: List[str], half1_map: List[str], half2_map: List[str], pdb_file: List[str], pdb_resolution: List[float]):
    """
        Run refinement and metrics (mtriage, emringer, qscore) for a given map type.

        Parameters
        ----------
        map_type : str
            Type of map to process (e.g., 'dem1', 'average_maps', etc.)
        maps : list[str]
            List of map file paths.
        half1_map : list[str]
            List of half-map 1 files (only used for average maps).
        half2_map : list[str]
            List of half-map 2 files (only used for average maps).
        pdb_file : list[str]
            List of PDB files corresponding to each map.
        pdb_resolution : list[float]
            List of resolution values corresponding to each PDB.

        Returns
        -------
        None
        """

    if not os.path.exists(output_directory):
        os.makedirs(output_directory)

    if not os.path.exists(extra_directory):
        os.makedirs(extra_directory)

    # Having all files from the different directories
    for i in range(len(maps)):
        # Keeps the name of the map
        pdb_file_name = os.path.basename(pdb_file[i]).split(".")[0]
        log_file_name = os.path.basename(maps[i]).split("_")[0]

        print(f"\n=== Processing {log_file_name} ({map_type}) ===")

        # Step 1: Run refinement if requested
        if args.refine:
            refined_pdb = run_real_space_refine(map_path=maps[i], pdb_file=pdb_file[i], pdb_file_name=pdb_file_name,
                                                pdb_resolution=pdb_resolution[i], log_file_name=log_file_name, map_type=map_type)

            if refined_pdb is None:
                print('---------------------------------ERROR---------------------------------')
                print(f'Mtriage and emringer metrics cannot be calculated due to lack of refined pdb '
                      f'for {log_file_name} map. Skip the rest')
                print('--------------------------------------------------------------------------------')
                continue  # Skip metrics if the refinement fails

            # Step 2: Run Mtriage & EMRinger with the refined pdb
            run_mtriage_emringer(map_path=maps[i], half1_map=half1_map[i], half2_map=half2_map[i], refined_pdb=refined_pdb,
                                 pdb_file_name=pdb_file_name, log_file_name=log_file_name, map_type=map_type,
                                 refine_first=True)

            # Step 3: Run Q-score with the refined pdb
            run_qscore(map_path=maps[i], pdb_path=refined_pdb, pdb_resolution=pdb_resolution[i], log_file_name=log_file_name,
                        output_directory=output_directory, extra_directory=extra_directory, map_type=map_type,
                       refined=True)

        else:
            # Calculate qscore with original pdb
            run_qscore(map_path=maps[i], pdb_path=pdb_file[i], pdb_resolution=pdb_resolution[i], log_file_name=log_file_name,
                        output_directory=output_directory, extra_directory=extra_directory, map_type=map_type,
                       refined=False)

            # Execute mtriage and emringer first with original PDB
            run_mtriage_emringer(map_path=maps[i], half1_map=half1_map[i], half2_map=half2_map[i],
                                 refined_pdb=pdb_file[i],
                                 pdb_file_name=pdb_file_name, log_file_name=log_file_name, map_type=map_type,
                                 refine_first=False)

            # Then calculate real_space_refine
            run_real_space_refine(map_path=maps[i], pdb_file=pdb_file[i], pdb_file_name=pdb_file_name,
                                  pdb_resolution=pdb_resolution[i], log_file_name=log_file_name,
                                  map_type=map_type)


def metrics_worker(args):
    """Wrapper function to run 'metrics' on a single map in parallel."""
    map_type, map_file, half1, half2, pdb_file, pdb_resolution = args
    metrics(map_type, [map_file], [half1], [half2], [pdb_file], [pdb_resolution])


def parallelize_metrics(map_type, maps, half1_maps, half2_maps, pdb_files, pdb_resolutions, num_workers=args.nw):
    """
    Run metric calculations (refinement, mtriage, emringer, qscore) in parallel for multiple maps.

    Parameters
    ----------
    map_type : str
        Type of map being processed (e.g., 'dem1', 'average_maps', etc.)
    maps : list[str]
        List of map file paths.
    half1_maps : list[str]
        List of half-map 1 file paths.
    half2_maps : list[str]
        List of half-map 2 file paths.
    pdb_files : list[str]
        List of PDB file paths corresponding to each map.
    pdb_resolutions : list[float]
        List of resolution values corresponding to each PDB.
    num_workers : int
        Number of parallel workers to use.
    """
    print('~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~')
    print(f'Number of workers being used: {num_workers}')
    print('~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~')

    # Prepare argument list for parallel processing
    task_args = [
        (map_type, maps[i], half1_maps[i], half2_maps[i], pdb_files[i], pdb_resolutions[i])
        for i in range(len(maps))
    ]

    # Execute tasks in parallel
    with concurrent.futures.ProcessPoolExecutor(max_workers=num_workers) as executor:
        futures = {executor.submit(metrics_worker, args): args for args in task_args}

        for future in concurrent.futures.as_completed(futures):
            task_info = futures[future]
            map_name = os.path.basename(task_info[1])
            try:
                future.result()
                print(f"[SUCCESS] Metrics successfully computed for {map_name}")
            except Exception as e:
                print(f"[ERROR] Failed to compute metrics for {map_name}: {e}")


# ---------------------- MAIN ----------------------
if __name__ == "__main__":
    print("Selected map type:", args.m)
    print('~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~')

    # === Search for files only when running the main script ===
    routes = read_routes(args.p)

    root_directory = routes['root_directory']
    output_directory = routes['output_directory']
    output_directory_dem1 = routes['output_directory_dem1']
    output_directory_emr = routes['output_directory_emr']
    output_directory_emr2 = routes['output_directory_emr2']
    output_directory_cryoten = routes['output_directory_cryoten']
    output_directory_locspiral = routes['output_directory_locspiral']
    output_directory_locscale_ = routes['output_directory_locscale_']
    output_directory_other = routes['output_directory_other']
    extra_directory = routes['extra_directory']
    json_file_path = routes['json_file_path']
    code_py_path = routes['code_py_path']

    # Print the paths of all the directories
    print("Root Directory:", root_directory)
    print("Output Directory:", output_directory)
    print("Output Directory for DEM1 maps:", output_directory_dem1)
    print("Output Directory for EMR maps:", output_directory_emr)
    print("Output Directory for EMR2 maps:", output_directory_emr2)
    print("Output Directory for CRYOTEN maps:", output_directory_cryoten)
    print("Output Directory for LOCSPIRAL maps:", output_directory_locspiral)
    print("Output Directory for LOCSCALE- maps:", output_directory_locscale_)
    print("Output Directory for OTHER maps:", output_directory_other)
    print("Extra Directory:", extra_directory)
    print("JSON File Path:", json_file_path)
    print("Python File Path:", code_py_path)

    # Make `files` accessible to functions below
    global files

    # === Now actually search for input files ===
    files = find_files(root_directory, json_file_path)

    if args.m == 'all':
        #pass
        for key in ["average_maps", "dem1", "emr", "emr2", "cryoten", "locspiral", "locscale", "locscale-", "other"]:

            maps, pdb_file, pdb_resolution = equal_length(key, files[key])
            parallelize_metrics(key, maps, files["half1_map"], files["half2_map"], pdb_file, pdb_resolution)

    else:
        maps, pdb_file, pdb_resolution = equal_length(args.m, files[args.m])
        parallelize_metrics(args.m, maps, files["half1_map"], files["half2_map"], pdb_file, pdb_resolution)

