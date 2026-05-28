import pandas as pd
import matplotlib.pyplot as plt
import re, os, argparse


parser = argparse.ArgumentParser(description="Process a Q-score residue file and plot per-residue Q-scores.")
parser.add_argument("archive", type=str, help="Path to the input .txt file")
parser.add_argument("--output_dir", type=str,
                                default="./plots_metrics_residues/",
                                help="Directory to save plots (default: ./plots_metrics_residues/)")
parser.add_argument("--not_refine", action="store_true",
                                help="If set, add 'not_refine' to output filename")
args = parser.parse_args()

def extract_qpeak_formula(lines):
    """
    Extract the Q_peak formula from a list of text lines.

    This function searches through the provided lines for a line starting with "Q_peak".
    Once found, it extracts the formula after the equals sign and converts any
    POWER(x, y) expressions to Python's exponentiation syntax (x**y).

    Args:
        lines (list[str]): A list of strings, each representing a line from a text file.

    Returns:
        str: The Q_peak formula as a Python-compatible string.

    Raises:
        ValueError: If no line starting with "Q_peak" is found in the input lines.
    """

    for line in lines:
        if line.strip().startswith("Q_peak"):
            match = re.search(r'Q_peak\s*=\s*(.+)', line)
            if match:
                raw_formula = match.group(1)
                # Replace POWER(x,y) with x**y for Python
                python_formula = re.sub(r'POWER\(([^,]+),\s*([^)]+)\)', r'(\1**\2)', raw_formula)
                return python_formula
    raise ValueError("The Q_peak formula was not found in the file.")


def process_file(filename):
    """
    Process a text file containing EMDB per-residue Q scores and Q_peak formula.

    This function performs the following steps:
    1. Reads all lines from the specified file.
    2. Extracts the resolution value from the line containing "Resolution entered".
    3. Extracts the Q_peak formula using `extract_qpeak_formula` and evaluates it using the resolution.
    4. Parses per-residue data (including Q_BackBone, Q_SideChain, Q_Residue, Q_Peak, Q_Low, Q_High) into a DataFrame.
    5. Returns the DataFrame along with Q_peak, Q_low, Q_high, and the resolution.

    Args:
        filename (str): Path to the input text file.

    Returns:
        tuple:
            - df (pd.DataFrame): DataFrame containing per-residue Q scores.
            - q_peak (float): Evaluated Q_peak value.
            - q_low (float): Q_low value from the first residue (representative).
            - q_high (float): Q_high value from the first residue (representative).
            - resolution (float): Resolution extracted from the file.

    Raises:
        ValueError: If the resolution or Q_peak formula is not found in the file.
    """

    with open(filename, 'r') as f:
        lines = f.readlines()

    # Extract resolution
    resolution = None
    for line in lines:
        if 'Resolution entered' in line:
            match = re.search(r'RES\):\s*([\d\.]+)', line)
            if match:
                resolution = float(match.group(1))
                break
    if resolution is None:
        raise ValueError("Resolution not found.")

    # Extract Q_peak fórmula
    formula = extract_qpeak_formula(lines)
    q_peak = eval(formula, {'RES': resolution})

    # Extract per-residue data (including Q_low and Q_high)
    data_lines = []
    for line in lines:
        # Detect lines with residue rows containing Q columns
        if re.match(r'^[A-Za-z]\t[A-Z]{3}\t\d+', line):
            cols = line.strip().split('\t')
            if len(cols) >= 9:  # make sure there are enough columns
                data_lines.append(cols[:9])

    df = pd.DataFrame(data_lines, columns=[
        'Chain', 'Residue', 'ResNum', 'Q_BackBone', 'Q_SideChain',
        'Q_Residue', 'Q_Peak', 'Q_Low', 'Q_High'
    ])
    df['ResNum'] = df['ResNum'].astype(int)
    df['Q_BackBone'] = pd.to_numeric(df['Q_BackBone'], errors='coerce')
    df['Q_SideChain'] = pd.to_numeric(df['Q_SideChain'], errors='coerce')
    df['Q_Residue'] = pd.to_numeric(df['Q_Residue'], errors='coerce')
    df['Q_Peak'] = pd.to_numeric(df['Q_Peak'], errors='coerce')
    df['Q_Low'] = pd.to_numeric(df['Q_Low'], errors='coerce')
    df['Q_High'] = pd.to_numeric(df['Q_High'], errors='coerce')

    # Take a representative value (e.g. from the first row) to plot horizontal lines
    q_low = df['Q_Low'].iloc[0]
    q_high = df['Q_High'].iloc[0]

    print(f'Q_peak formula: {formula}, \n Q_peak values: {q_peak}, \n Q_low: {q_low} \n Q_high: {q_high}')

    return df, q_peak, q_low, q_high, resolution


def extract_map_method(filename):
    """
    Extract the EMDB map identifier and processing method from a filename.

    This function parses the filename to identify:
    1. The EMDB map ID, expected in the format "EMD-XXXX".
    2. The processing method, such as cryoten, emready, locscale, locspiral, deepemhancer, etc.

    Args:
        filename (str): The name of the file containing the map and method info.

    Returns:
        tuple:
            - emd_map (str): The EMDB map ID (e.g., "EMD-1234") or "UnknownMap" if not found.
            - method (str): The method extracted from the filename, or "average_map" if not found.
    """
    # Extract EMDB map (EMD-XXXX)
    map_match = re.search(r'(EMD-\d+)', filename)
    emd_map = map_match.group(1) if map_match else "UnknownMap"

    # Extract method (cryoten, dem1, emready, locscale, locspiral)
    method_match = re.search(r'_(dem1|cryoten|locscale-|locscale|locspiral|emr|emr2)[._]', filename)
    method = method_match.group(1) if method_match else "average_map"

    # Dictionary for renaming methods
    method_rename = {
        "dem1": "dem",
        "locscale-": "locscale*"
    }
    # Replace method name if it's in the dictionary
    method = method_rename.get(method, method)

    return emd_map, method



def plot(df, q_peak, q_low, q_high, resolution):
    """
    Plot per-residue Q-scores with reference lines for Q_peak, Q_low, and Q_high.

    This function generates a scatter plot of Q_BackBone and Q_SideChain scores
    for each amino acid residue in the DataFrame. Horizontal lines indicate
    Q_peak, Q_low, and Q_high values. The plot is saved to the output directory
    with a filename derived from the EMDB map ID and processing method.

    Args:
        df (pd.DataFrame): DataFrame containing per-residue Q scores.
        q_peak (float): The evaluated Q_peak value.
        q_low (float): Representative Q_low value (e.g., first residue).
        q_high (float): Representative Q_high value (e.g., first residue).
        resolution (float): The resolution of the map in Ångströms.

    Returns:
        None
    """

    # Extract EMDB map ID and processing method from archive filename
    emd_map, method = extract_map_method(os.path.basename(archive))

    # Create figure and axis
    fig, ax = plt.subplots(figsize=(10, 6))

    ax.scatter(df['ResNum'], df['Q_SideChain'], s=10, color='orange', label='Q_SideChain')
    ax.scatter(df['ResNum'], df['Q_BackBone'], s=10, color='deepskyblue', label='Q_BackBone')

    ax.axhline(y=q_peak, color='black', linestyle='-', label=f'Q_Peak @{resolution:.2f}Å')
    ax.axhline(y=q_low, color='black', linestyle='--', dashes=(2,2), label=f'Q_Low_95%@{resolution:.2f}Å')
    ax.axhline(y=q_high, color='black', linestyle='--', dashes=(4,4), label=f'Q_High_95%@{resolution:.2f}Å')

    # Axis labels
    ax.set_xlabel('Amino acid residue #', fontsize=20)
    ax.set_ylabel('Q-score', fontsize=20)
    title_suffix = "not_refine" if args.not_refine else ""
    ax.set_title(f'Q-score per residue {method} {title_suffix}', fontsize=28)

    ax.tick_params(axis='x', labelsize=20)
    ax.tick_params(axis='y', labelsize=20)

    # Extract legend elements and save separately
    handles, labels = ax.get_legend_handles_labels()
    save_auto_legend(handles, labels, emd_map)

    fig.tight_layout()

    # Save figure
    suffix = "_not_refine" if args.not_refine else ""
    filename = f"{emd_map}_{method}{suffix}.tiff"
    fig.savefig(os.path.join(output_directory, filename), dpi=300, bbox_inches="tight")
    plt.show()


def save_auto_legend(handles, labels, emd_map):
    """
        Generate and save a standalone legend figure for Q-score plots.

        This function creates a separate figure containing only the legend
        corresponding to a plot (typically generated in `plot`). The legend
        is saved as a TIFF file to avoid cluttering the main plot and to allow
        flexible reuse in publications or figure layouts.

        If a legend file for the given EMDB map already exists, the function
        skips regeneration.

        Notes:
            - This function depends on external/global variables:
                * `args.not_refine`: flag to modify filename
                * `output_directory`: directory where the legend is saved
            - Intended to be used with matplotlib handles and labels extracted
              from an existing plot.

        Args:
            handles (list):
                List of matplotlib artist handles (e.g., from ax.get_legend_handles_labels()).

            labels (list):
                List of corresponding labels for the legend entries.

            emd_map (str):
                EMDB map identifier used to construct the output filename.

        Returns:
            None
        """

    # Define filename suffix based on refinement flag
    suffix = "_not_refine" if args.not_refine else ""
    filename = f"legend_{emd_map}{suffix}.tiff"
    filepath = os.path.join(output_directory, filename)

    # Avoid regenerating legend if it already exists
    if os.path.exists(filepath):
        print(f"Legend already exists for {emd_map}, skipping...")
        return

    # Create a small figure dedicated to the legend
    fig_legend = plt.figure(figsize=(6, 1))

    # Add an empty axis (needed to host the legend)
    ax_leg = fig_legend.add_subplot(111)
    ax_leg.axis('off')

    # Create legend centered in the figure
    fig_legend.legend(
        handles,
        labels,
        loc='center',
        ncol=5,                 # Arrange legend entries in columns
        frameon=False,          # Remove legend border
        fontsize=12
    )

    # Save legend as a high-resolution TIFF image
    fig_legend.savefig(
        filepath,
        dpi=300,
        bbox_inches='tight'
    )


archive = args.archive
output_directory = args.output_dir
os.makedirs(output_directory, exist_ok=True)  # Create folder if it does not exist

df, q_peak, q_low, q_high, res = process_file(archive)
plot(df, q_peak, q_low, q_high, res)
