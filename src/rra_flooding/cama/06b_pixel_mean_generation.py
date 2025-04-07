import pandas as pd # type: ignore
from pathlib import Path
import numpy as np # type: ignore
import argparse

parser = argparse.ArgumentParser(description="Run James code")

# Define arguments
parser.add_argument("--hierarchy", type=str, required=True, help="Hierarchy")
parser.add_argument("--scenario", type=str, required=True, help="Scenario")
parser.add_argument("--variant", type=str, required=True, help="Variant")


# Parse arguments
args = parser.parse_args()

hierarchy = args.hierarchy
scenario = args.scenario
variant = args.variant

def create_mean_results(hierarchy: str, scenario: str, variant: str = "r1i1p1f1") -> None:
    root = Path("/mnt/team/rapidresponse/pub/flooding/results/output/") / hierarchy
    models = [
        "ACCESS-CM2", "EC-Earth3", "INM-CM5-0", "MIROC6", 
        "IPSL-CM6A-LR", "NorESM2-MM", "MRI-ESM2-0",
    ]
    # models = ["GFDL-CM4"] # remove becuase not in ssp126

    df_list = []

    for model in models:
        file_name = f"flood_fraction_sum_std_{scenario}_{model}_{variant}.parquet"
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
    output_file = root / f"flood_fraction_sum_std_{scenario}_mean_{variant}.parquet"
    combined_df.to_parquet(output_file, index=False)
    # change file permissions to 775
    output_file.chmod(0o775)

# Call the function with parsed arguments
create_mean_results(
    hierarchy=hierarchy,
    scenario=scenario,
    variant=variant,
)
