import os, subprocess
import shutil
import argparse, json

# ============================
# ARGUMENT PARSER
# ============================
parser = argparse.ArgumentParser(description='Apply locscale to a single EMD map (.mrc)')
parser.add_argument('-m', type=str, required=True,
                    help='Path to the EMD map (.mrc) to apply locscale to')
parser.add_argument('-o', type=str, required=True,
                    help='Path of the output map')
parser.add_argument('-p', type=str, required=True,
                    help='Path where the archive of path files is located')

args = parser.parse_args()

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

routes = read_routes(args.p)
tmp_folder_locscale = routes['tmp_folder_locscale']
processing_files_locscale = routes['processing_files_locscale']


# Create a temporary folder if it does not exist
os.makedirs(tmp_folder_locscale, exist_ok=True)

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
        subprocess.run(command, shell=True, check=True)
        print(f'Command executed successfully: {" ".join(command)}')
    except subprocess.CalledProcessError as e:
        print(f'Error executing command: {" ".join(command)}')
        print(e)



def process_map(map_path):
    """
        Process an EMDB map using locscale and a corresponding PDB file.

        This function performs the following steps:
        1. Validates that the input file has a '.mrc' extension.
        2. Extracts the EMD number from the filename and prints progress.
        3. Checks for the corresponding JSON metadata and PDB file.
        4. Copies the map to a temporary folder for processing.
        5. Executes the locscale command with appropriate arguments.
        6. Cleans up temporary and problematic files after processing.

        Parameters
        ----------
        map_path (str): Path to the EMDB map file (.mrc) to process.

        Returns
        ----------
        None
        """
    if not map_path.endswith('.mrc'):
        print(f'Archive not valid: {map_path}')
        return

    filename = os.path.basename(map_path)
    emd_number = filename.split('_')[0]
    print(f'\nProcessing {filename} (EMD: {emd_number})')
    print('~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~')

    base_dir = os.path.dirname(os.path.dirname(map_path))

    json_path = os.path.join(base_dir, 'info', f'{emd_number}.json')
    if not os.path.exists(json_path):
        print(f'JSON not found for {emd_number}')
        return

    with open(json_path, 'r') as file:
        data = json.load(file)

    map_resolution = data['resolution']
    pdb_name = data['pdbs'][0]
    pdb_path = os.path.join(base_dir, 'pdbs', f'{pdb_name}.pdb')

    if not os.path.exists(pdb_path):
        print(f'PDB not found for {emd_number}')
        return

    if os.path.exists(args.o):
        print(f'{args.o} already exists, skipping...')
        return

    tmp_archive = os.path.join(tmp_folder_locscale, filename)
    shutil.copy2(map_path, tmp_archive)
    print(f'Copied {map_path} to {tmp_archive}')

    locscale_command = (
        f'locscale -em {tmp_archive} '
        f'-mc {pdb_path} '
        f'-o {args.o} '
        f'-mres {map_resolution} --skip_refine '
    )

    print(f'Running locscale...')
    print(f'LOCSCALE COMMAND {locscale_command}')

    run_command(locscale_command)

    if os.path.exists(tmp_archive):
        os.remove(tmp_archive)
        print(f'Removed temporary archive: {tmp_archive}')
        print('----------------------------------')
        print('----------------------------------')

    # Remove problematic files from processing
    for file in [filename, os.path.basename(pdb_path)]:
        file_path = os.path.join(processing_files_locscale, file)
        if os.path.exists(file_path):
            os.remove(file_path)
            print(f'Removed {file_path} due to issues in calculating metrics')


# Process the single map
process_map(args.m)