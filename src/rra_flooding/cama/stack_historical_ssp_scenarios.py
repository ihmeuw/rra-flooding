import os
import numpy as np # type: ignore
import pandas as pd # type: ignore
import xarray as xr # type: ignore
from rra_tools.shell_tools import mkdir, touch # type: ignore
from pathlib import Path
import argparse
import yaml

# Create the argument parser
parser = argparse.ArgumentParser(description="Generate daily netcdf bricks for each model and scenario.")

# Define arguments
parser.add_argument("--model", type=str, required=True, help="Climate model name")
parser.add_argument("--variable", type=str, required=True, help="Variable to process")
parser.add_argument("--adjustment_num", type=int, required=True, help="Adjustment number")

# Parse arguments
args = parser.parse_args()

# Define root directory
INPUT_ROOT = Path("/mnt/team/rapidresponse/pub/flooding/output/")
OUTPUT_ROOT = Path("/mnt/team/rapidresponse/pub/flooding/results/annual/raw")
mkdir(OUTPUT_ROOT, parents=True, exist_ok=True)

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


def stack_historical_with_ssp(model: str, variable: str, adjustment_num: int) -> None:
    """
    Stacks the historical NetCDF brick with each SSP scenario NetCDF brick for a given model.
    """
    variable_dict = parse_yaml_dictionary(variable, adjustment_num)
    variable = variable_dict['variable']
    covariate = variable_dict['covariate']

    historical_path = INPUT_ROOT / variable /"historical" / model / f"stacked_{covariate}.nc"

    # Check if historical file exists
    if not historical_path.exists():
        print(f"❌ Historical file not found for model {model}: {historical_path}")
        return

    # Load historical dataset
    ds_historical = xr.open_dataset(historical_path)

    # Define SSP scenarios
    ssp_scenarios = ["ssp126", "ssp245", "ssp585"]
    ssp_files = [INPUT_ROOT / variable / scenario / model / f"stacked_{covariate}.nc" for scenario in ssp_scenarios]

    # Filter only existing SSP scenario files
    valid_ssp_files = [(scenario, file) for scenario, file in zip(ssp_scenarios, ssp_files) if file.exists()]

    if not valid_ssp_files:
        print(f"❌ No SSP files found for model {model}")
        return
    
    print(f"Available SSP scenarios for {model}: {', '.join([s[0] for s in valid_ssp_files])}")

    for scenario, file in valid_ssp_files:
        # Load SSP dataset
        ds_ssp = xr.open_dataset(file)

        # Concatenate historical and SSP along time dimension
        ds_combined = xr.concat([ds_historical, ds_ssp], dim="time")

        # Define output path
        output_dir = OUTPUT_ROOT / scenario / covariate
        mkdir(output_dir, parents=True, exist_ok=True)
        output_file = output_dir / f"{model}.nc"
        touch(output_file, clobber=True, mode=0o775)

        # Define encoding for compression
        encoding = {
            "value": {"zlib": True, "complevel": 5, "dtype": "float32"},
            "lon": {"dtype": "float32", "zlib": True, "complevel": 5},
            "lat": {"dtype": "float32", "zlib": True, "complevel": 5},
            "time": {"dtype": "int32", "zlib": True, "complevel": 5}
        }

        # Save the combined NetCDF
        ds_combined.to_netcdf(output_file, format="NETCDF4", encoding=encoding)
        os.chmod(output_file, 0o775) # temporary


def clean_up_stacked_ssp_files(model: str, scenario: str, variable: str, adjustment_num: int) -> None:
    """
    Removes yearly summary NetCDF files for a given model and scenario.
    """
    variable_dict = parse_yaml_dictionary(variable, adjustment_num)
    summary_statistic = variable_dict['summary_statistic']
    variable = variable_dict['variable']
    covariate = variable_dict['covariate']

    input_dir = INPUT_ROOT / variable / scenario / model

    # Get all yearly NetCDF files
    netcdf_files = input_dir.glob(f"stacked_{covariate}.nc")

    for f in netcdf_files:
        f.unlink()
        print(f"❌ Removed: {f}")

def main(model: str, variable: str, adjustment_num: int) -> None:
    """Runs individual steps in sequence."""
    stack_historical_with_ssp(model, variable, adjustment_num)
    # clean_up_stacked_ssp_files(model, scenario, variable, adjustment_num)

# Run main function with parsed arguments
main(args.model, args.variable, args.adjustment_num)