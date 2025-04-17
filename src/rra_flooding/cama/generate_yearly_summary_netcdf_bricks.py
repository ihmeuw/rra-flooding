import os
import numpy as np # type: ignore
import xarray as xr # type: ignore
import pandas as pd # type: ignore
from pathlib import Path
from rra_tools.shell_tools import mkdir, touch # type: ignore
import argparse

# Create the argument parser
parser = argparse.ArgumentParser(description="Run flooding model standardization for multiple years.")

# Define arguments
parser.add_argument("--variable", type=str, required=True, help="Variable to process")
parser.add_argument("--model", type=str, required=True, help="Climate model name")
parser.add_argument("--scenario", type=str, required=True, help="Climate scenario")
parser.add_argument("--summary_statistic", type=str, required=True, help="Summary statistic to apply to the covariate")
parser.add_argument("--threshold", type=float, required=False, default=1.0, help="Threshold for count_over_threshold statistic")
parser.add_argument("--variant", type=str, default="r1i1p1f1", help="Model variant identifier")

# Parse arguments
args = parser.parse_args()

def create_yearly_summary_netcdf(model: str, scenario: str, variable:str, summary_statistic: str, threshold: float = 1.0, variant: str = "r1i1p1f1") -> None:
    """Creates yearly summary NetCDF files by summing daily flood fraction values while adding a time dimension."""

    original_variable = variable.str.split("_")[0]
    root = Path(f"/mnt/team/rapidresponse/pub/flooding/output/{original_variable}/")
    input_dir = root / scenario / model

    new_covariate = f"{variable}_{summary_statistic}" 
    nodata = -9999  # Define the nodata value

    if scenario == "historical":
        start_year, end_year = 1970, 2014
    else:
        start_year, end_year = 2015, 2100

    for year in range(start_year, end_year + 1):
        input_file = input_dir / f"{variable}_{year}.nc"

        if not input_file.exists():
            print(f"❌ Skipping {year}, input file not found: {input_file}")
            continue

        # Load dataset
        ds = xr.open_dataset(input_file)

        # Mask nodata values (-9999) by converting them to NaN
        ds[variable] = ds[variable].where(ds[variable] != nodata, np.nan)

        # Create variable-based summary statistic
        if summary_statistic == "sum":
            ds_yearly = ds.sum(dim="time", skipna=True)
        elif summary_statistic == "mean":
            ds_yearly = ds.mean(dim="time", skipna=True)
        elif summary_statistic == "median":
            ds_yearly = ds.median(dim="time", skipna=True)
        elif summary_statistic == "max":
            ds_yearly = ds.max(dim="time", skipna=True)
        elif summary_statistic == "min":
            ds_yearly = ds.min(dim="time", skipna=True)
        elif summary_statistic == "count_over_threshold":
            if threshold is None:
                raise ValueError("Threshold must be provided for 'count_over_threshold' statistic.")
            ds_yearly = (ds[variable] > threshold).sum(dim="time", skipna=True)
        else:
            raise ValueError(
                f"Unsupported summary_statistic: '{summary_statistic}'. "
                "Choose from: 'sum', 'mean', 'median', 'max', 'min', 'count_over_threshold'."
            )

        # Rename variable
        ds_yearly = ds_yearly.rename({variable: new_covariate})

        # Add a single time value corresponding to the mid-year timestamp
        ds_yearly = ds_yearly.expand_dims("time")  # Add time dimension
        ds_yearly["time"] = [np.datetime64(f"{year}-07-01")]  # Assign timestamp

        # Replace NaNs back with nodata (-9999)
        ds_yearly[new_covariate] = ds_yearly[new_covariate].fillna(nodata)

        # Define encoding for compression
        encoding = {
            new_covariate: {"zlib": True, "complevel": 5, "dtype": "float32", "_FillValue": nodata},
            "lon": {"dtype": "float32", "zlib": True, "complevel": 5},
            "lat": {"dtype": "float32", "zlib": True, "complevel": 5},
            "time": {"dtype": "int32", "units": "days since 1900-01-01", "zlib": True, "complevel": 5},  # Define time format
        }

        output_file = input_dir / f"{new_covariate}_{year}.nc"
        touch(output_file, clobber=True, mode=0o775)

        # Save the yearly summary NetCDF
        ds_yearly.to_netcdf(output_file, format="NETCDF4", engine="netcdf4", encoding=encoding)

def stack_yearly_netcdf(model: str, scenario: str, variable:str, summary_statistic: str) -> None:
    """
    Stacks yearly NetCDF files for a given model and scenario into a single NetCDF file.
    """

    original_variable = variable.str.split("_")[0]
    root = Path(f"/mnt/team/rapidresponse/pub/flooding/output/{original_variable}/")
    input_dir = root / scenario / model

    new_covariate = f"{variable}_{summary_statistic}" 

    # Get all yearly NetCDF files
    netcdf_files = sorted(input_dir.glob(f"{new_covariate}_*.nc"))  # Sorting ensures proper time order

    if not netcdf_files:
        print(f"❌ No NetCDF files found for {model} - {scenario}")
        return

    # Extract years from filenames
    years = np.array([int(f.stem.split("_")[-1]) for f in netcdf_files], dtype=np.int32)  # Convert to NumPy int32 array

    # Load datasets
    ds_list = [xr.open_dataset(f) for f in netcdf_files]

    # Concatenate along new 'time' dimension
    ds_stacked = xr.concat(ds_list, dim="time")
    ds_stacked = ds_stacked.assign_coords(time=("time", years))  # Set years as time

    # Define encoding (compress all variables)
    encoding = {var: {"zlib": True, "complevel": 5, "dtype": "float32"} for var in ds_stacked.data_vars}
    encoding.update({
        "time": {"dtype": "int32"},  # Remove "units" from encoding
        "lon": {"dtype": "float32", "zlib": True, "complevel": 5},
        "lat": {"dtype": "float32", "zlib": True, "complevel": 5},
    })

    output_file = input_dir / scenario / model / f"stacked_{new_covariate}.nc"
    touch(output_file, clobber=True, mode=0o775)

    # Save stacked NetCDF
    ds_stacked.to_netcdf(output_file, format="NETCDF4", encoding=encoding)

def clean_up_yearly_netcdf_files(model: str, scenario: str, variable: str, summary_statistic: str) -> None:
    """
    Removes yearly summary NetCDF files for a given model and scenario.
    """
    original_variable = variable.str.split("_")[0]
    root = Path(f"/mnt/team/rapidresponse/pub/flooding/output/{original_variable}/")
    input_dir = root / scenario / model

    new_covariate = f"{variable}_{summary_statistic}"
    # Get all yearly NetCDF files
    netcdf_files = input_dir.glob(f"{new_covariate}_*.nc")

    for f in netcdf_files:
        f.unlink()
        print(f"❌ Removed: {f}")

def main(model: str, scenario: str, variable: str, summary_statistic: str, threshold: float, variant: str) -> None:
    """Runs individual steps in sequence."""
    create_yearly_summary_netcdf(model, scenario, variable, summary_statistic, threshold, variant)
    stack_yearly_netcdf(model, scenario, variable, summary_statistic)
    clean_up_yearly_netcdf_files(model, scenario, variable, summary_statistic)

# Run main function with parsed arguments
main(args.model, args.scenario, args.variable, args.summary_statistic, args.threshold, args.variant)
