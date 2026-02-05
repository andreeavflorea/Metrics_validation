import mrcfile
import sys
import subprocess
import os, json, argparse
import numpy as np
from scipy.ndimage import binary_dilation, gaussian_filter

sys.path.append('/home/andreea/Software/scipion-em-ucm/ucm')

from computations import LocSpiral

# ============================
# ARGUMENT PARSER
# ============================
parser = argparse.ArgumentParser(description='Select from which maps you want to calculate locspiral'
                                             '\n example of code:'
                                             '\n python3 locspiral.py -t locspiral '
                                             '-m /media/andreea/DATA1/dem2_data_andreea/average_maps/EMD-0004_full.mrc '
                                             '-o /media/andreea/DATA1/dem2_data_andreea/locspiral/EMD-0004_locspiral.mrc ')

parser.add_argument(
    '-t', type=str, required=True, choices='locspiral',
    help=(
        "LocSpiral type:\n"
        "  locspiral  - Calculates locspiral map\n"
    )
)
parser.add_argument('-m', type=str, required=True, help='Path to the input map (average_map).')
parser.add_argument('-o', type=str, required=True, help='Path for the output map.')

args = parser.parse_args()

# -----------------------------
# File and Path Handling
# -----------------------------
vol_file = args.m
#pdb_file_path = args.r
locspiral_path = args.o

emd_id = os.path.basename(vol_file).split('_')[0].split('.')[0]
json_path = os.path.join(os.path.dirname(os.path.dirname(vol_file)), 'info', f'{emd_id}.json')

print(f"Processing EMD entry: {emd_id}")
print(f"Running mode: {args.t}")
print(f"Reading metadata from: {json_path}")

# -----------------------------
# Load metadata
# -----------------------------
with open(json_path, 'r') as file:
    data = json.load(file)

resolution = data['resolution']
voxel_size = data['voxel_size']

# -----------------------------
# Parameter Configuration
# -----------------------------
if args.t == 'locspiral':
    resolution = resolution
    voxel_size = voxel_size
    min_res = 25
    max_res = resolution - 0.05*resolution
    num_points = 15
    noise_threshold = 0.9
    f_voxel_width = 5
    nthreads = 10

    print(f"Map resolution: {resolution} | Max res: {max_res}")

else:
    sys.exit("Error: Unrecognized LocSpiral type.")

# ============================
# LOAD MAPS
# ============================
vol = mrcfile.open(vol_file).data

emr2_map_path = os.path.join(os.path.dirname(os.path.dirname(vol_file)), "emr2_maps",
                             f'{emd_id}_full_emr2.mrc')

if not os.path.exists(emr2_map_path):
    sys.exit(f"Error: EMR2 map not found at {emr2_map_path}")

print(f'Using {emr2_map_path}')
emr2_map = mrcfile.open(emr2_map_path).data
mask = emr2_map > 1.5

# ============================
# RUN LOCSPIRAL
# ============================
loc, amplitude = LocSpiral(vol, mask,
                           voxel_size=voxel_size,
                           min_res=min_res,
                           max_res=max_res,
                           protein_threshold=noise_threshold,
                           f_voxel_width=f_voxel_width,
                           num_points=num_points,
                           masking=False)

num_pixels_extend = 3
sigma = 1
mask = binary_dilation(mask, iterations=num_pixels_extend)
mask = gaussian_filter(mask.astype(np.float32), sigma=sigma)

loc = loc * mask

# ============================
# SAVE OUTPUT
# ============================
with mrcfile.new(locspiral_path, overwrite=True) as mrc:
    mrc.set_data(loc)
    mrc.voxel_size = voxel_size
print(f"LocSpiral map saved at: {locspiral_path}")