import os
import numpy as np # type: ignore
import xarray as xr # type: ignore
import pandas as pd # type: ignore
from pathlib import Path
from rra_tools.shell_tools import mkdir # type: ignore
import argparse

# Create the argument parser
parser = argparse.ArgumentParser(description="Run flooding model standardization for multiple years.")

# Define arguments
parser.add_argument("--model", type=str, required=True, help="Climate model name")
parser.add_argument("--scenario", type=str, required=True, help="Climate scenario")
parser.add_argument("--variant", type=str, default="r1i1p1f1", help="Model variant identifier")

# Parse arguments
args = parser.parse_args()

covariate = "fldfrc_weighted"
new_covariate = "fldfrc_weighted_sum"

OUTPUT_ROOT = Path("/mnt/team/rapidresponse/pub/flooding/output/fldfrc")

def create_yearly_summary_netcdf(model: str, scenario: str, variant: str = "r1i1p1f1") -> None:
    """Creates yearly summary NetCDF files by summing daily flood fraction values while adding a time dimension."""
    
    input_dir = OUTPUT_ROOT / scenario / model
    output_dir = OUTPUT_ROOT / scenario / model
    mkdir(output_dir, parents=True, exist_ok=True)

    nodata = -9999  # Define the nodata value

    if scenario == "historical":
        start_year, end_year = 1970, 2014
    else:
        start_year, end_year = 2015, 2100

    for year in range(start_year, end_year + 1):
        input_file = input_dir / f"{covariate}_{year}.nc"
        output_file = output_dir / f"{new_covariate}_{year}.nc"

        if not input_file.exists():
            print(f"❌ Skipping {year}, input file not found: {input_file}")
            continue

        # Load dataset
        ds = xr.open_dataset(input_file)

        # Mask nodata values (-9999) by converting them to NaN
        ds[covariate] = ds[covariate].where(ds[covariate] != nodata, np.nan)

        # Sum over time, ignoring NaNs
        ds_yearly = ds.sum(dim="time", skipna=True)

        # Rename variable from "fldfrc_weighted" to "fldfrc_weighted_sum"
        ds_yearly = ds_yearly.rename({covariate: new_covariate})

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

        # if file exists, delete it
        if output_file.exists():
            output_file.unlink()
            
        # Save the yearly summary NetCDF
        ds_yearly.to_netcdf(output_file, format="NETCDF4", engine="netcdf4", encoding=encoding)

        # Set file permissions
        os.chmod(output_file, 0o775)

        print(f"✅ Yearly summary saved: {output_file}")

def stack_yearly_netcdf(model: str, scenario: str) -> None:
    """
    Stacks yearly NetCDF files for a given model and scenario into a single NetCDF file.
    """
    # Define paths
    input_dir = OUTPUT_ROOT / scenario / model
    output_file = OUTPUT_ROOT / scenario / model / f"stacked_{new_covariate}.nc"

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

    # If file exists, delete it
    if output_file.exists():
        output_file.unlink()

    # Save stacked NetCDF
    ds_stacked.to_netcdf(output_file, format="NETCDF4", encoding=encoding)

    # Set file permissions to 0o775
    os.chmod(output_file, 0o775)

    print(f"✅ Stacked NetCDF saved at: {output_file}")

def clean_up_yearly_netcdf_files(model: str, scenario: str) -> None:
    """
    Removes yearly summary NetCDF files for a given model and scenario.
    """
    input_dir = OUTPUT_ROOT / scenario / model

    # Get all yearly NetCDF files
    netcdf_files = input_dir.glob(f"{new_covariate}_*.nc")

    for f in netcdf_files:
        f.unlink()
        print(f"❌ Removed: {f}")

def main(model: str, scenario: str, variant: str) -> None:
    """Runs individual steps in sequence."""
    create_yearly_summary_netcdf(model, scenario, variant)
    stack_yearly_netcdf(model, scenario)
    clean_up_yearly_netcdf_files(model, scenario)

# Run main function with parsed arguments
main(args.model, args.scenario, args.variant)