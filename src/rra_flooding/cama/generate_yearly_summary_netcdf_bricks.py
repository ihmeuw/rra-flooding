import os
import numpy as np # type: ignore
import xarray as xr # type: ignore
import pandas as pd # type: ignore
from pathlib import Path
from rra_tools.shell_tools import mkdir, touch # type: ignore
from rra_flooding.data import FloodingData
from rra_flooding import constants as rfc
from rra_flooding.helper_functions import parse_yaml_dictionary, load_yaml_dictionary
import argparse
import yaml # type: ignore

# Create the argument parser
parser = argparse.ArgumentParser(description="Run flooding model standardization for multiple years.")

# Define arguments
parser.add_argument("--model", type=str, required=True, help="Climate model name")
parser.add_argument("--scenario", type=str, required=True, help="Climate scenario")
parser.add_argument("--variant", type=str, default="r1i1p1f1", help="Model variant identifier")
parser.add_argument("--variable", type=str, required=True, help="variable to process")
parser.add_argument("--adjustment_num", type=int, required=True, help="Which adjustment to apply")
parser.add_argument("--model_root", type=str, default=rfc.MODEL_ROOT, help="Root of the model directory")
# Parse arguments
args = parser.parse_args()

def create_yearly_summary_netcdf(model: str, scenario: str, variant: str, variable: str, adjustment_num: int, model_root: str) -> None:
    """Creates yearly summary NetCDF files by summing daily flood fraction values while adding a time dimension."""
    floodingdata = FloodingData(model_root)

    variable_dict = parse_yaml_dictionary(variable, adjustment_num)
    adjusted_variable = variable_dict['adjusted_variable']
    summary_statistic = variable_dict['summary_statistic']
    summary_variable = variable_dict['summary_variable']


    if summary_statistic == "countoverthreshold":
        threshold = variable_dict.get("threshold")
    

    root = Path(f"/mnt/team/rapidresponse/pub/flooding/output/{variable}/")
    input_dir = root / scenario / model

    nodata = -9999  # Define the nodata value

    if scenario == "historical":
        start_year, end_year = 1970, 2014
    else:
        start_year, end_year = 2015, 2100

    for year in range(start_year, end_year + 1):
        input_file_path = floodingdata.output_path(variable, scenario, model, year, variable_name = adjusted_variable)
        

        if not input_file_path.exists():
            print(f"❌ Skipping {year}, input file not found: {input_file_path}")
            continue

        # Load dataset
        variable_ds = floodingdata.load_output(variable, scenario, model, year, variable_name = adjusted_variable)

        # Mask nodata values (-9999) by converting them to NaN
        variable_ds["value"] = variable_ds["value"].where(variable_ds["value"] != nodata, np.nan)

        # Create variable-based summary statistic
        if summary_statistic == "sum":
            variable_ds_yearly = variable_ds.sum(dim="time", skipna=True)
        elif summary_statistic == "mean":
            variable_ds_yearly = variable_ds.mean(dim="time", skipna=True)
        elif summary_statistic == "median":
            variable_ds_yearly = variable_ds.median(dim="time", skipna=True)
        elif summary_statistic == "max":
            variable_ds_yearly = variable_ds.max(dim="time", skipna=True)
        elif summary_statistic == "min":
            variable_ds_yearly = variable_ds.min(dim="time", skipna=True)
        elif summary_statistic == "countoverthreshold":
            if threshold is None:
                raise ValueError("Threshold must be provided for 'countoverthreshold' statistic.")
            variable_ds_yearly = (variable_ds["value"] > threshold).sum(dim="time", skipna=True)
        else:
            raise ValueError(
                f"Unsupported summary_statistic: '{summary_statistic}'. "
                "Choose from: 'sum', 'mean', 'median', 'max', 'min', 'countoverthreshold'."
            )

        # Add a single time value corresponding to the mid-year timestamp
        variable_ds_yearly = variable_ds_yearly.expand_dims("time")  # Add time dimension
        variable_ds_yearly["time"] = [np.datetime64(f"{year}-07-01")]  # Assign timestamp

        # Replace NaNs back with nodata (-9999)
        variable_ds_yearly["value"] = variable_ds_yearly["value"].fillna(nodata)

        floodingdata.save_output(variable_ds_yearly, variable, scenario, model, year, variable_name = summary_variable)

def stack_yearly_netcdf(model: str, scenario: str, variable: str, adjustment_num: int, model_root: str) -> None: 
    """
    Stacks yearly NetCDF files for a given model and scenario into a single NetCDF file.
    """
    floodingdata = FloodingData(model_root)

    variable_dict = parse_yaml_dictionary(variable, adjustment_num)
    summary_variable = variable_dict['summary_variable']

    root = Path(f"/mnt/team/rapidresponse/pub/flooding/output/{variable}/")
    input_dir = root / scenario / model


    # Get all yearly NetCDF files
    netcdf_files = sorted(input_dir.glob(f"{summary_variable}_*.nc"))  # Sorting ensures proper time order

    if not netcdf_files:
        print(f"❌ No NetCDF files found for {model} - {scenario}")
        return

    # Extract years from filenames
    years = np.array([int(f.stem.split("_")[-1]) for f in netcdf_files], dtype=np.int32)  # Convert to NumPy int32 array

    # Load datasets
    variable_ds_list = [xr.open_dataset(f) for f in netcdf_files]

    # Concatenate along new 'time' dimension
    variable_ds_stacked = xr.concat(variable_ds_list, dim="time")
    variable_ds_stacked = variable_ds_stacked.assign_coords(time=("time", years))  # Set years as time

    floodingdata.save_stacked_output(variable_ds_stacked, variable, scenario, model, variable_name = f"stacked_{summary_variable}")


def clean_up_yearly_netcdf_files(model: str, scenario: str, variable: str, adjustment_num: int, model_root: str) -> None:
    """
    Removes yearly summary NetCDF files for a given model and scenario.
    """
    floodingdata = FloodingData(model_root)

    variable_dict = parse_yaml_dictionary(variable, adjustment_num)
    summary_variable = variable_dict['summary_variable']

    root = Path(f"/mnt/team/rapidresponse/pub/flooding/output/{variable}/")
    input_dir = root / scenario / model

    # Get all yearly NetCDF files
    netcdf_files = input_dir.glob(f"{summary_variable}_*.nc")

    for f in netcdf_files:
        f.unlink()
        print(f"❌ Removed: {f}")

def main(model: str, scenario: str, variant: str, variable: str, adjustment_num: int, model_root: str) -> None:
    """Runs individual steps in sequence."""

    create_yearly_summary_netcdf(model, scenario, variant, variable, adjustment_num, model_root)
    stack_yearly_netcdf(model, scenario, variable, adjustment_num, model_root)
    clean_up_yearly_netcdf_files(model, scenario, variable, adjustment_num, model_root)

# Run main function with parsed arguments
main(args.model, args.scenario, args.variant, args.variable, args.adjustment_num, args.model_root)
