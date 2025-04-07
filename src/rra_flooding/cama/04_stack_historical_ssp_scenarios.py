import os
import numpy as np # type: ignore
import pandas as pd # type: ignore
import xarray as xr # type: ignore
from rra_tools.shell_tools import mkdir # type: ignore
from pathlib import Path
import argparse

# Create the argument parser
parser = argparse.ArgumentParser(description="Generate daily netcdf bricks for each model and scenario.")

# Define arguments
parser.add_argument("--model", type=str, required=True, help="Climate model name")
parser.add_argument("--scenario", type=str, required=True, help="Climate scenario")

# Parse arguments
args = parser.parse_args()

# Define root directory
INPUT_ROOT = Path("/mnt/team/rapidresponse/pub/flooding/output/fldfrc")
OUTPUT_ROOT = Path("/mnt/team/rapidresponse/pub/flooding/results/annual/raw")
OUTCOME = "fldfrc_weighted_sum"  # The variable to be stacked

def stack_historical_with_ssp(model: str) -> None:
    """
    Stacks the historical NetCDF brick with each SSP scenario NetCDF brick for a given model.
    """
    historical_path = INPUT_ROOT / "historical" / model / "stacked_{OUTCOME}.nc"

    # Check if historical file exists
    if not historical_path.exists():
        print(f"❌ Historical file not found for model {model}: {historical_path}")
        return

    # Load historical dataset
    ds_historical = xr.open_dataset(historical_path)

    # Define SSP scenarios
    ssp_scenarios = ["ssp126", "ssp245", "ssp585"]
    ssp_files = [INPUT_ROOT / scenario / model / f"stacked_{OUTCOME}.nc" for scenario in ssp_scenarios]

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
        output_file = OUTPUT_ROOT / scenario / OUTCOME / f"{model}.nc"
        output_file.parent.mkdir(parents=True, exist_ok=True)  # Ensure directory exists

        # Define encoding for compression
        encoding = {
            "OUTCOME": {"zlib": True, "complevel": 5, "dtype": "float32"},
            "lon": {"dtype": "float32", "zlib": True, "complevel": 5},
            "lat": {"dtype": "float32", "zlib": True, "complevel": 5},
            "time": {"dtype": "int32", "zlib": True, "complevel": 5}
        }

        # If file exists, delete it
        if output_file.exists():
            output_file.unlink()

        # Save the combined NetCDF
        ds_combined.to_netcdf(output_file, format="NETCDF4", encoding=encoding)

        # Set file permissions
        os.chmod(output_file, 0o775)

        print(f"✅ Historical and {scenario} stacked NetCDF saved: {output_file}")

def clean_up_stacked_ssp_files(model: str, scenario: str) -> None:
    """
    Removes yearly summary NetCDF files for a given model and scenario.
    """
    input_dir = OUTPUT_ROOT / scenario / model

    # Get all yearly NetCDF files
    netcdf_files = input_dir.glob("stacked_{OUTCOME}.nc")

    for f in netcdf_files:
        f.unlink()
        print(f"❌ Removed: {f}")

def main(model: str, scenario: str) -> None:
    """Runs individual steps in sequence."""
    stack_historical_with_ssp(model)
    # clean_up_stacked_ssp_files(model, scenario)

# Run main function with parsed arguments
main(args.model, args.scenario)