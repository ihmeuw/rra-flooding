import pandas as pd # type: ignore
from pathlib import Path
import numpy as np # type: ignore
import argparse
import yaml # type: ignore

parser = argparse.ArgumentParser(description="Run James code")

# Define arguments
parser.add_argument("--variable", type=str, required=True, help="Variable to process")
parser.add_argument("--adjustment_num", type=int, required=True, help="Adjustment number")
parser.add_argument("--hierarchy", type=str, required=True, help="Hierarchy")
parser.add_argument("--scenario", type=str, required=True, help="Scenario")
parser.add_argument("--variant", type=str, default="r1i1p1f1", help="Model variant identifier")


# Parse arguments
args = parser.parse_args()

variable = args.variable
adjustment_num = args.adjustment_num
hierarchy = args.hierarchy
scenario = args.scenario
variant = args.variant


SCRIPT_ROOT = Path.cwd()
REPO_ROOT = Path(str(SCRIPT_ROOT).split("rra-flooding")[0] + "rra-flooding")

def parse_yaml_dictionary(variable: str, adjustment_num: str) -> dict:
    # Read YAML
    with open(REPO_ROOT / 'src' / 'rra_flooding'  / 'VARIABLE_DICT.yaml', 'r') as f:
        yaml_data = yaml.safe_load(f)

    # Extract variable-specific config
    variable_dict = yaml_data['VARIABLE_DICT']
    variable_list = variable_dict.get(variable, [])
    if adjustment_num >= len(variable_list):
        raise IndexError(f"Adjustment number {adjustment_num} out of range for variable '{variable}'")

    entry = variable_list[adjustment_num]

    # Build the return dict dynamically
    result = {
        "variable": variable,
        "summary_statistic": entry['summary_statistic']['type'],
        "adjustment_type": entry['adjustment']['type'],
        "covariate": f"{variable}_{entry['adjustment']['type']}"
    }

    if entry['summary_statistic']['type'] == "countoverthreshold":
        result['threshold'] = entry['summary_statistic'].get("threshold")

    if entry['adjustment']['type'] == "shifted":
        result["shift_type"] = entry['adjustment'].get("shift_type")
        result["shift"] = entry['adjustment'].get("shift")
        result["covariate"] = f"{variable}_{entry['adjustment']['type']}{entry['adjustment']['shift']}_{entry['summary_statistic']['type']}"

    return result

variable_dict = parse_yaml_dictionary(variable, adjustment_num)
variable = variable_dict['variable']
covariate = variable_dict['covariate']
OUTCOME = covariate  # The variable to be stacked

def create_mean_results(hierarchy: str, scenario: str, variant: str) -> None:
    root = Path("/mnt/team/rapidresponse/pub/flooding/results/output/") / hierarchy
    models = [
        "ACCESS-CM2", "EC-Earth3", "INM-CM5-0", "MIROC6", 
        "IPSL-CM6A-LR", "NorESM2-MM", "MRI-ESM2-0",
    ]
    # models = ["GFDL-CM4"] # remove becuase not in ssp126

    df_list = []

    for model in models:
        file_name = f"{covariate}_{scenario}_{model}_{variant}.parquet"
        file_path = root / file_name
        if not file_path.exists():
            continue 
        
        df = pd.read_parquet(file_path)
        df_list.append(df)

    if not df_list:
        return 

    combined_df = pd.concat(df_list, ignore_index=True)
    combined_df["model"] = "mean"

    combined_df = combined_df.groupby(
        ["location_id", "year_id", "model", "scenario", "variant"]
    ).agg({
        "people_flood_days": "mean", 
        "people_flood_days_per_capita": "mean",
        "population": "mean"
    }).reset_index()

    # Save the aggregated mean results
    output_file = root / f"{covariate}_{scenario}_mean_{variant}.parquet"
    combined_df.to_parquet(output_file, index=False)
    # change file permissions to 775
    output_file.chmod(0o775)

# Call the function with parsed arguments
create_mean_results(
    hierarchy=hierarchy,
    scenario=scenario,
    variant=variant,
)
