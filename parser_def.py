import os, glob, locale
import re
import pandas as pd
import argparse

# Set locale for proper sorting if needed
locale.setlocale(locale.LC_COLLATE, '')

# ----------------------------
# ARGUMENT PARSER
# ----------------------------
parser = argparse.ArgumentParser(description='Select which name you want for the csv that contains all the metrics')
parser.add_argument('-o', type=str, required= True,
                    help='Name of the csv that contains all the metrics (without the ending .csv, just the name)')
parser.add_argument('-p', type=str, required= True,
                    help='Path where the archive of path files is located')
args = parser.parse_args()


# ----------------------------
# FUNCTION TO READ PATHS FILE
# ----------------------------
def read_routes(file_path):
    """
        Reads a file with lines in the format key = 'value' and returns a dictionary
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

# Directory containing the metric files
directory = routes['output_directory']
print("Directory with the metrics:", directory)

# Output CSV path
output_folder_csv = os.path.join(routes['output_folder_csv'], args.o + '.csv')
print("Carpeta CSV File:", output_folder_csv)

# List to store all extracted data
all_data = []

# ----------------------------
# FUNCTION TO EXTRACT DATA
# ----------------------------
def extract_metrics(patterns, content, **flags):
    """
        Extracts data from 'content' according to provided regex patterns and optional flags.

        Flags: locscale, locscale-, dem1, emr, emr2, cryoten, locspiral, other, not_refine
        """
    row = {}
    suffix = '_not_refine' if flags.get('not_refine', False) else ''

    # Map flags to their suffix strings
    flag_suffix = {
        'locscale': 'locscale',
        'locscale-': 'locscale-',
        'dem1': 'dem1',
        'emr': 'emr',
        'emr2': 'emr2',
        'cryoten': 'cryoten',
        'other': 'other',
        'locspiral': 'locspiral'
    }

    # Determine which flag is active
    active_flag = next((key for key, val in flags.items() if key in flag_suffix and val), None)
    base_suffix = flag_suffix.get(active_flag, '')

    for key, pattern in patterns.items():
        match = re.search(pattern, content, re.DOTALL)
        num_groups = re.compile(pattern).groups

        if match:
            # If there are two groups (masked/unmasked)
            if len(match.groups()) == 2:
                masked_val, unmasked_val = match.group(1), match.group(2)
                if base_suffix:
                    row[f"{key}_masked_{base_suffix}{suffix}"] = masked_val
                    row[f"{key}_unmasked_{base_suffix}{suffix}"] = unmasked_val
                else:
                    row[f"{key}_masked{suffix}"] = masked_val
                    row[f"{key}_unmasked{suffix}"] = unmasked_val
            else:
                # Single group
                if base_suffix:
                    row[f"{key}_{base_suffix}{suffix}"] = match.group(1)
                else:
                    row[f"{key}{suffix}"] = match.group(1)

        else:
            # No match: fill with None
            if len(match.groups() if match else ()) == 2:
                if base_suffix:
                    row[f"{key}_masked_{base_suffix}{suffix}"] = None
                    row[f"{key}_unmasked_{base_suffix}{suffix}"] = None
                else:
                    row[f"{key}_masked{suffix}"] = None
                    row[f"{key}_unmasked{suffix}"] = None
            else:
                if base_suffix:
                    row[f"{key}_{base_suffix}{suffix}"] = None
                else:
                    row[f"{key}{suffix}"] = None

    return row



# ----------------------------
# METRIC PATTERNS
# ----------------------------
patterns_real_space_refined = {
    "CC_mask": r'model-to-map fit, CC_mask *: *(None|-?\d+\.\d+)',
    "moved_start": r'moved from start *: *(None|-?\d+\.\d+)',
    "clashscore": r'All-atom Clashscore *: *(None|-?\d+\.\d+)',
    "ramachandran_outliers": r'\s*Outliers\s *: *(None|-?\d+\.\d+)',
    "ramachandran_allowed": r'\s*Allowed\s *: *(None|-?\d+\.\d+)',
    "ramachandran_favored": r'\s*Favored\s  *: *(None|-?\d+\.\d+)',
    "rotamer_outliers": r'Rotamer:\s*.*Outliers\s*:\s*(None|-?\d+\.\d+)',
    "rotamer_allowed": r'Rotamer:\s*.*Allowed\s*:\s*(None|-?\d+\.\d+)',
    "rotamer_favored": r'Rotamer:\s*.*Favored\s*:\s*(None|-?\d+\.\d+)'
}

patterns_emringer = {
    "optimal_threshold": r'Optimal Threshold *: *(None|-?\d+\.\d+)',
    "rotamer_ratio": r'Rotamer-Ratio *: *(None|-?\d+\.\d+)',
    "max_zscore": r'Max Zscore *: *(None|-?\d+\.\d+)',
    "len_model": r'Model Length *: *(None|-?\d+)',
    "emringer_score": r'EMRinger Score *: *(None|-?\d+\.\d+)'
}

patterns_mtriage = {
    "d99": r'using map alone \(d99\) *: *(None|-?\d+\.\d+) *(None|-?\d+\.\d+)',
    "d_model": r'comparing with model \(d_model\) *: *(None|-?\d+\.\d+) *(None|-?\d+\.\d+)',
    "b_iso_overall": r'b_iso_overall *: *(None|-?\d+\.\d+) *(None|-?\d+\.\d+)',
    "d_model_b0": r'comparing with model \(d_model_b0\) *: *(None|-?\d+\.\d+) *(None|-?\d+\.\d+)',
    "d_fsc_model_0": r'FSC\(map,model map\)=0 *: *(None|-?\d+\.\d+) *(None|-?\d+\.\d+)',
    "d_fsc_model_0143": r'FSC\(map,model map\)=0\.143 *: *(None|-?\d+\.\d+) *(None|-?\d+\.\d+)',
    "d_fsc_model_05": r'FSC\(map,model map\)=0\.5 *: *(None|-?\d+\.\d+) *(None|-?\d+\.\d+)',
    "d99_half_map1": r'd99 \(half map 1\) *: *(None|-?\d+\.\d+) *(None|-?\d+\.\d+)',
    "d99_half_map2": r'd99 \(half map 2\) *: *(None|-?\d+\.\d+) *(None|-?\d+\.\d+)',
    "d_fsc_half_maps": r'FSC\(half map 1,2\)=0\.143 \(d_fsc\) *: *(None|-?\d+\.\d+) *(None|-?\d+\.\d+)',
}

patterns_qscore = {
    "qscore_avg": r"overall average Q:\s*(None|-?\d+\.\d+)"
    }


# ----------------------------
# SUFFIXES FOR LOG FILES
# ----------------------------
mtriage_suffixes = [
    'mtriage.log', 'mtriage_locscale.log', 'mtriage_locscale-.log', 'mtriage_dem1.log',
    'mtriage_emr.log', 'mtriage_emr2.log', 'mtriage_cryoten.log', 'mtriage_locspiral.log', 'mtriage_other.log',
    'mtriage_not_refine.log', 'mtriage_locscale_not_refine.log', 'mtriage_locscale-_not_refine.log',
    'mtriage_dem1_not_refine.log', 'mtriage_emr_not_refine.log', 'mtriage_emr2_not_refine.log',
    'mtriage_cryoten_not_refine.log', 'mtriage_locspiral_not_refine.log', 'mtriage_other_not_refine.log'
]

emringer_suffixes = [s.replace("mtriage", "emringer") for s in mtriage_suffixes]
real_space_refined_suffixes = [s.replace("mtriage", "real_space_refined") for s in mtriage_suffixes]
qscore_suffixes = [s.replace("mtriage", "qscore_avg") for s in mtriage_suffixes]



# ----------------------------
# PROCESS LOG FILES
# ----------------------------
for input_file_path in sorted(glob.glob(os.path.join(directory, "*.log")), key=locale.strxfrm):
    filename = os.path.basename(input_file_path)
    if not os.path.isfile(input_file_path):
        continue

    with open(input_file_path, 'r') as file:
        content = file.read()

    # Extract the map name from the filename
    map_name = filename.split('.')[1].split('_')[0]
    row = {"Map": map_name}

    # Determine active flags from filename
    flags = {
        'dem1': 'dem1' in filename,
        'emr': 'emr.' in filename or 'emr_' in filename,
        'emr2': 'emr2' in filename,
        'cryoten': 'cryoten' in filename,
        'locspiral': 'locspiral.' in filename or 'locspiral_' in filename,
        'locscale-': 'locscale-' in filename,
        'other': 'other' in filename,
        'locscale': 'locscale' in filename and 'locscale-' not in filename,
        'not_refine': 'not_refine' in filename
    }

    if filename.endswith(tuple(mtriage_suffixes)):
        extracted_data = extract_metrics(patterns_mtriage, content, **flags)
        if not any(metric is not None for metric in extracted_data.values()):
            print(f"[WARNING] No mtriage metrics were found in: {filename}. Please check the initial archive")
            continue
        else:
            row.update(extracted_data)

    elif filename.endswith(tuple(emringer_suffixes)):
        row.update(extract_metrics(patterns_emringer, content, **flags))

    elif filename.endswith(tuple(real_space_refined_suffixes)):
        lines = content.splitlines()
        positions = [i for i, line in enumerate(lines) if 'Overall statistics' in line]

        if len(positions) >= 2:
            # Extract the text between the first two appearances (before_refine block)
            text_before = '\n'.join(lines[positions[0]:positions[1]])
            row.update({
                f"{key}_before_refine": value
                for key, value in
                extract_metrics(patterns_real_space_refined, text_before, **flags).items()
            })

            # Extract the last block as after_refine
            text_after = '\n'.join(lines[positions[-1]:])
            row.update({
                f"{key}_after_refine": value
                for key, value in
                extract_metrics(patterns_real_space_refined, text_after, **flags).items()
            })


        elif len(positions) == 1:
            # Only one block found; assume before_refine
            print(
                f"[WARNING] Only one 'Overall statistics' block found in  {filename}. Assuming before_refine.")
            text_single = '\n'.join(lines[positions[0]:])
            row.update({
                f"{key}_before_refine": value
                for key, value in
                extract_metrics(patterns_real_space_refined, text_single, **flags).items()
            })

    elif filename.endswith(tuple(qscore_suffixes)):
        row.update(extract_metrics(patterns_qscore, content, **flags))

    all_data.append(row)

# ----------------------------
# CREATE DATAFRAME AND SAVE CSV
# ----------------------------
df = pd.DataFrame(all_data)

# Group by map to remove duplicates, keep first occurrence, and reset index
df = df.groupby('Map').first().reset_index()

df.to_csv(output_folder_csv) #, index=False
print(f"Data successfully saved to {output_folder_csv}")





