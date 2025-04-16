import os
import numpy as np # type: ignore
import pandas as pd # type: ignore
import xarray as xr # type: ignore
from rra_tools.shell_tools import mkdir, touch # type: ignore
from pathlib import Path
import argparse

# Create the argument parser
parser = argparse.ArgumentParser(description="Generate daily netcdf bricks for each model and scenario.")

# Define arguments
parser.add_argument("--model", type=str, required=True, help="Climate model name")
parser.add_argument("--scenario", type=str, required=True, help="Climate scenario")
parser.add_argument("--variable", type=str, required=True, help="Variable to process")

# Parse arguments
args = parser.parse_args()

# Define root directory
INPUT_ROOT = Path("/mnt/team/rapidresponse/pub/flooding/output/")
OUTPUT_ROOT = Path("/mnt/team/rapidresponse/pub/flooding/results/annual/raw")

def stack_historical_with_ssp(model: str, variable: str) -> None:
    """
    Stacks the historical NetCDF brick with each SSP scenario NetCDF brick for a given model.
    """
    original_variable = variable.split("_")[0]
    historical_path = INPUT_ROOT / original_variable /"historical" / model / f"stacked_{variable}.nc"

    # Check if historical file exists
    if not historical_path.exists():
        print(f"❌ Historical file not found for model {model}: {historical_path}")
        return

    # Load historical dataset
    ds_historical = xr.open_dataset(historical_path)

    # Define SSP scenarios
    ssp_scenarios = ["ssp126", "ssp245", "ssp585"]
    ssp_files = [INPUT_ROOT / original_variable / scenario / model / f"stacked_{variable}.nc" for scenario in ssp_scenarios]

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
        output_dir = OUTPUT_ROOT / scenario / variable
        mkdir(output_dir, parents=True, exist_ok=True)
        output_file = output_dir / f"{model}.nc"
        touch(output_file, clobber=True, mode=0o775)

        # Define encoding for compression
        encoding = {
            variable: {"zlib": True, "complevel": 5, "dtype": "float32"},
            "lon": {"dtype": "float32", "zlib": True, "complevel": 5},
            "lat": {"dtype": "float32", "zlib": True, "complevel": 5},
            "time": {"dtype": "int32", "zlib": True, "complevel": 5}
        }

        # Save the combined NetCDF
        ds_combined.to_netcdf(output_file, format="NETCDF4", encoding=encoding)


def clean_up_stacked_ssp_files(model: str, scenario: str, variable: str) -> None:
    """
    Removes yearly summary NetCDF files for a given model and scenario.
    """
    original_variable = variable.split("_")[0]
    input_dir = INPUT_ROOT / original_variable / scenario / model

    # Get all yearly NetCDF files
    netcdf_files = input_dir.glob("stacked_{OUTCOME}.nc")

    for f in netcdf_files:
        f.unlink()
        print(f"❌ Removed: {f}")

def main(model: str, scenario: str, variable: str) -> None:
    """Runs individual steps in sequence."""
    stack_historical_with_ssp(model, variable)
    # clean_up_stacked_ssp_files(model, scenario, variable)

# Run main function with parsed arguments
main(args.model, args.scenario, args.variable)