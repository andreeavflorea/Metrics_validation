import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
from collections import defaultdict
import re, json, argparse, os


# ================================================================
# ------------    Functions to compute matches    ----------------
# ================================================================
def extract_number(emdb_id):
    """
        Extract the numeric part of an EMDB identifier.

        This function searches for a numeric sequence within an EMDB ID string
        formatted as "EMD-XXXX" (for example, "EMD-1234"). If a match is found,
        the numeric part is returned as an integer. If no match is found, -1 is returned.

        Parameters
        ----------
        emdb_id (str): The EMDB identifier string (e.g., "EMD-1234").

        Returns
        ----------
        int: The numeric part of the EMDB ID if found; otherwise, -1.
    """
    match = re.search(r'EMD-(\d+)', emdb_id)
    return int(match.group(1)) if match else -1


def load_test_ids(routes_json):
    """
        Load test IDs from a list of JSON files.

        This function reads each JSON file provided in `routes_json`,
        extracts the value associated with the key "test" (if it exists),
        and combines all these values into a single list.

        Parameters
        ----------
        routes_json (list[str]): A list of file paths pointing to JSON files.

        Returns
        ----------
        list: A list containing all test IDs found in the provided JSON files.
    """

    test_ids = []
    for route in routes_json:
        with open(route, 'r') as f:
            data = json.load(f)
            test_ids.extend(data.get("test", []))
    return test_ids



def generate_sets():
    """
    Generate sets of matching and non-matching EMDB IDs between different datasets.

    This function performs the following steps:
    1. Loads test IDs from the Seneca dataset JSON files and extracts numeric parts.
    2. Loads CryoTen dataset CSV and extracts the map numbers for training and validation sets.
    3. Loads EMReady dataset spreadsheet and extracts training and validation map numbers.
    4. Compares the Seneca test IDs against CryoTen and EMReady training/validation sets
       to determine which IDs are matches and which are non-matches.

    Returns
    ----------
    tuple: Two sets:
        - matches: EMDB IDs present in either CryoTen or EMReady training/validation sets.
        - non_matches: EMDB IDs not present in CryoTen or EMReady training/validation sets.
    """


    # -------- 1. Test set (Seneca) ----------------
    # >>> USER: update the path to the JSON file defining the dataset splits
    routes_seneca = [
        "/media/andreea/DATA1/dem2_data_andreea/sep2024_train_val_test_split.json"
    ]

    seneca_test_ids = load_test_ids(routes_seneca)
    seneca_test_nums = sorted(set([extract_number(e) for e in seneca_test_ids]))

    # -------- 2. CryoTen ----------------
    # >>> USER: update the path to the CryoTEN and EMReady partition CSV files
    df_cryoten = pd.read_csv("dataset_cryoten.csv")
    df_cryoten["Map_Number"] = df_cryoten["EMDB Map"].str.extract(r"EMD-(\d+)", expand=False).astype(int)

    cryoten_train_val = set(
        df_cryoten[df_cryoten["split"].isin(["train", "val"])]["Map_Number"]
    )

    # -------- 3. EMReady ----------------
    # >>> USER: update the path to the CryoTEN and EMReady partition CSV files
    df_emready = pd.read_excel("train_val_test_split_emready.ods", engine="odf", dtype=str)

    emready_train_val = set()
    for col in ["train", "val"]:
        emready_train_val.update(
            pd.to_numeric(df_emready[col], errors="coerce").dropna().astype(int).tolist()
        )

    # -------- 4. Matches / Non-matches ----------------
    matches, non_matches = set(), set()
    for num in seneca_test_nums:
        if num in cryoten_train_val or num in emready_train_val:
            matches.add(f"EMD-{str(num).zfill(4)}")
        else:
            non_matches.add(f"EMD-{str(num).zfill(4)}")

    print('---------------------------------')
    print('Matching and non-matching maps from the half0 and half1 test sets in the training and validation sets '
          'of emready and cryoten.')
    print('---------------------------------')
    print(f'matches {matches}, \nlength {len(matches)}')
    print(f'non_matches {non_matches}, \nlength {len(non_matches)}')

    return matches, non_matches


# ==========================================
# ------------    PARSER    ----------------
# ==========================================
parser = argparse.ArgumentParser(description="Create metric boxplots with interactive filtering.")
parser.add_argument(
    "--file_path",
    type=str,
    default="all_metrics_test.csv",
    help="Path to the metrics CSV file."
)
parser.add_argument(
    "--output_dir",
    type=str,
    default="./plots_metrics/",
    help="Output directory for plots."
)
parser.add_argument(
    "--stats_file",
    type=str,
    default="stats_complete.json",
    help="Path to the stats file."
)
parser.add_argument(
    "--mode",
    choices=["all", "matches", "non_matches"],
    default="all",
    help="Metric filtering mode: all (entire CSV), matches, or non_matches."
)
args = parser.parse_args()

output_dir = args.output_dir if args.output_dir else "./plots_metrics/"
os.makedirs(output_dir, exist_ok=True)

df = pd.read_csv(args.file_path)

if args.mode in ["matches", "non_matches"]:
    matches, non_matches = generate_sets()
    if args.mode == "matches":
        print("Mode activated: filtering MATCHES only.")
        df = df[df["Map"].isin(matches)]
    else:
        print("Mode activated: filtering NON-MATCHES only.")
        df = df[df["Map"].isin(non_matches)]
    print(f"Saved {len(df)} maps after applying the '{args.mode}' filter.")
    filtered_csv = "metrics_non_matches.csv"
    #df.to_csv(filtered_csv, index=False)
    print(f"Non matches CSV saved in: {filtered_csv}")
else:
    print("Using all metrics without filtering.")



pattern = re.compile(
    r"^(.*?)(?:_(masked|unmasked))?"
    r"(?:_(dem1|cryoten|locscale-|locscale|locspiral|other|emr|emr2))?"
    r"(_not_refine)?"#
    r"(?:_(before_refine|after_refine))?$"
)

# >>> USER: modify this list to exclude specific methods from the boxplots
# IMPORTANT:
# The right-hand side of `in` must be a tuple to work correctly.
# ("emr") is NOT a tuple but a string → causes errors when match.group(3) is None.
# A valid 1-element tuple must include a comma, e.g. ("emr",).
# Use () (empty tuple) if you do not want to exclude any method.
filtered_columns = [
    col for col in df.columns
    if not ((match := pattern.match(col)) and match.group(3) in ())
]

df = df[filtered_columns]
sns.set(style="whitegrid")


column_groups = defaultdict(
    lambda: {
        "refine": defaultdict(list),
        "not_refine": defaultdict(list),
    }
)


for col in df.columns:
    match = pattern.match(col)
    if match:
        base = match.group(1)
        mask_status = match.group(2) or "none"
        method = match.group(3) or "average_maps"
        not_refine_flag = match.group(4)
        refine_stage = match.group(5)

        # Determine if it is refined or not
        refine_status = "not_refine" if not_refine_flag else "refine"
        # It can be masked, unmasked, before_refine, etc
        group_key = refine_stage if refine_stage else mask_status

        column_groups[base][refine_status][group_key].append((method, col))


for base, refine_dict in column_groups.items():
    print("~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~")
    print(f"METRIC BASE: {base}")

    for refine_status, groups in refine_dict.items():
        print(f"    Refinement: {refine_status.upper()}")

        for group_key, cols in groups.items():
            print(f"    ➤ Group: {group_key.upper()}")
            for method, column in cols:
                print(f"        • Method: {method} ➔ Column: {column}")


# ========================================
# ---------    STATISTICS    -------------
# ========================================
global_statistics = {}
skip_bases = ["Unnamed: 0", "Map"]

method_rename = {
    "dem1": "dem",
    "locscale-": "locscale*"
}

for base, refine_dict in column_groups.items():
    if base in skip_bases:
        continue

    refine_groups = refine_dict["refine"]
    not_refine_groups = refine_dict["not_refine"]

    all_keys = set(refine_groups.keys()).union(not_refine_groups.keys())

    # Case 1: Has masked/unmasked
    if "masked" in all_keys or "unmasked" in all_keys:
        fig, axes = plt.subplots(2, 2, figsize=(14, 10), sharey=True)
        fig.suptitle(f"{base} (masked/unmasked + refine)", fontsize=16)

        for i, (mask_status, refine_status, ax) in enumerate([
            ("masked", "refine", axes[0, 0]),
            ("masked", "not_refine", axes[1, 0]),
            ("unmasked", "refine", axes[0, 1]),
            ("unmasked", "not_refine", axes[1, 1])
        ]):
            cols = column_groups[base][refine_status].get(mask_status, [])
            if not cols:
                ax.set_visible(False)
                continue

            #df_plot = pd.DataFrame({
            #    f"{method}": df[col] for method, col in cols
            #})

            df_plot = pd.DataFrame({
                method_rename.get(method, method): df[col]
                for method, col in cols
            })

            print(f'Count Nan values\n {df_plot.isna().sum()}')
            sns.boxplot(data=df_plot, ax=ax, flierprops=dict(marker='o', markerfacecolor='none'))
            ax.set_title(f"{mask_status} - {refine_status}")
            ax.tick_params(axis='x', rotation=45, labelsize=9)

            # Statistics
            stats = df_plot.describe().T[['mean', '50%', '25%', '75%']].rename(columns={'50%': 'median'})
            print(f"\n Statistics for {base} ➔ {refine_status} ➔ {mask_status}:\n")
            print(stats, "\n")
            key = f"{base}_{mask_status}_{refine_status}"
            global_statistics[key] = stats

        plt.tight_layout(rect=[0, 0, 1, 0.95])
        filename = f"{base}.tiff"
        plt.savefig(f"{output_dir}{filename}", dpi=300, bbox_inches="tight")
        plt.show()

    # Case 2: Has before_refine / after_refine
    elif "before_refine" in all_keys or "after_refine" in all_keys:
        fig, axes = plt.subplots(2, 1, figsize=(10, 8), sharey=True)
        fig.suptitle(f"{base} (before/after refine)", fontsize=16)

        for i, refine_stage in enumerate(["before_refine", "after_refine"]):
            cols = refine_groups.get(refine_stage, [])
            if not cols:
                axes[i].set_visible(False)
                continue

            #df_plot = pd.DataFrame({
            #    f"{method}": df[col] for method, col in cols
            #})

            df_plot = pd.DataFrame({
                method_rename.get(method, method): df[col]
                for method, col in cols
            })

            print(f'Count Nan values\n {df_plot.isna().sum()}')
            sns.boxplot(data=df_plot, ax=axes[i], flierprops=dict(marker='o', markerfacecolor='none'))
            axes[i].set_title(refine_stage)
            axes[i].tick_params(axis='x', rotation=45, labelsize=9)

            stats = df_plot.describe().T[['mean', '50%', '25%', '75%']].rename(columns={'50%': 'median'})
            print(f"\n Statistics for {base} ➔ {refine_stage}:\n")
            print(stats, "\n")
            key = f"{base}_{refine_stage}"
            global_statistics[key] = stats

        plt.tight_layout(rect=[0, 0, 1, 0.95])
        filename = f"{base}.tiff"
        plt.savefig(f"{output_dir}{filename}", dpi=300, bbox_inches="tight")
        plt.show()

    # Case 3: Only refine / not_refine without masked/unmasked nor before/after
    elif "none" in all_keys:
        fig, axes = plt.subplots(2, 1, figsize=(10, 8), sharey=True)
        fig.suptitle(f"{base} (refine vs not_refine)", fontsize=16)

        for i, refine_status in enumerate(["refine", "not_refine"]):
            cols = column_groups[base][refine_status].get("none", [])
            if not cols:
                axes[i].set_visible(False)
                continue

            #df_plot = pd.DataFrame({
            #    f"{method}": df[col] for method, col in cols
            #})

            df_plot = pd.DataFrame({
                method_rename.get(method, method): df[col]
                for method, col in cols
            })

            print(f'Count Nan values\n {df_plot.isna().sum()}')
            sns.boxplot(data=df_plot, ax=axes[i], flierprops=dict(marker='o', markerfacecolor='none'))
            axes[i].set_title(refine_status)
            axes[i].tick_params(axis='x', rotation=45, labelsize=9)

            stats = df_plot.describe().T[['mean', '50%', '25%', '75%']].rename(columns={'50%': 'median'})
            print(f" \n Statistics for {base} ➔ {refine_status}:\n")
            print(stats, "\n")
            key = f"{base}_{refine_status}"
            global_statistics[key] = stats



        plt.tight_layout(rect=[0, 0, 1, 0.95])
        filename = f"{base}.tiff"
        plt.savefig(f"{output_dir}{filename}", dpi=300, bbox_inches="tight")
        plt.show()


json_statistics = {
    key: df_stats.to_dict() for key, df_stats in global_statistics.items()
}

# Save as JSON
with open(f"{args.stats_file}", "w") as f:
    json.dump(json_statistics, f, indent=4)
