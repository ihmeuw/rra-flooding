import os
import numpy as np # type: ignore
import xarray as xr # type: ignore
import pandas as pd # type: ignore
from pathlib import Path
from rra_tools.shell_tools import mkdir, touch # type: ignore
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
# Parse arguments
args = parser.parse_args()

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
        result["covariate"] = f"{variable}_{entry['adjustment']['type']}{entry['adjustment']['shift']}"

    

    return result

def create_yearly_summary_netcdf(model: str, scenario: str, variant: str, variable: str, adjustment_num: int) -> None:
    """Creates yearly summary NetCDF files by summing daily flood fraction values while adding a time dimension."""
    variable_dict = parse_yaml_dictionary(variable, adjustment_num)
    variable = variable_dict['variable']
    summary_statistic = variable_dict['summary_statistic']
    covariate = variable_dict['covariate']
    if summary_statistic == "countoverthreshold":
        threshold = variable_dict.get("threshold")
    

    root = Path(f"/mnt/team/rapidresponse/pub/flooding/output/{variable}/")
    input_dir = root / scenario / model

    new_covariate = f"{covariate}_{summary_statistic}" 
    nodata = -9999  # Define the nodata value

    if scenario == "historical":
        start_year, end_year = 1970, 2014
    else:
        start_year, end_year = 2015, 2100

    for year in range(start_year, end_year + 1):
        input_file = input_dir / f"{covariate}_{year}.nc"

        if not input_file.exists():
            print(f"❌ Skipping {year}, input file not found: {input_file}")
            continue

        # Load dataset
        ds = xr.open_dataset(input_file)

        # Mask nodata values (-9999) by converting them to NaN
        ds[covariate] = ds[covariate].where(ds[covariate] != nodata, np.nan)

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
            ds_yearly = (ds[covariate] > threshold).sum(dim="time", skipna=True)
        else:
            raise ValueError(
                f"Unsupported summary_statistic: '{summary_statistic}'. "
                "Choose from: 'sum', 'mean', 'median', 'max', 'min', 'count_over_threshold'."
            )

        # Rename variable
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

        output_file = input_dir / f"{new_covariate}_{year}.nc"
        touch(output_file, clobber=True, mode=0o775)
        # Save the yearly summary NetCDF
        ds_yearly.to_netcdf(output_file, format="NETCDF4", engine="netcdf4", encoding=encoding)
        os.chmod(output_file, 0o775)  # Temporary

def stack_yearly_netcdf(model: str, scenario: str, variable: str, adjustment_num: int) -> None: 
    """
    Stacks yearly NetCDF files for a given model and scenario into a single NetCDF file.
    """
    variable_dict = parse_yaml_dictionary(variable, adjustment_num)
    summary_statistic = variable_dict['summary_statistic']
    variable = variable_dict['variable']
    covariate = variable_dict['covariate']

    root = Path(f"/mnt/team/rapidresponse/pub/flooding/output/{variable}/")
    input_dir = root / scenario / model

    new_covariate = f"{covariate}_{summary_statistic}" 

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

    output_file = input_dir / f"stacked_{new_covariate}.nc"
    touch(output_file, clobber=True, mode=0o775)

    # Save stacked NetCDF
    ds_stacked.to_netcdf(output_file, format="NETCDF4", encoding=encoding)
    os.chmod(output_file, 0o775)  # Temporary

def clean_up_yearly_netcdf_files(model: str, scenario: str, variable: str, adjustment_num: int) -> None:
    """
    Removes yearly summary NetCDF files for a given model and scenario.
    """
    variable_dict = parse_yaml_dictionary(variable, adjustment_num)
    summary_statistic = variable_dict['summary_statistic']
    variable = variable_dict['variable']
    covariate = variable_dict['covariate']

    root = Path(f"/mnt/team/rapidresponse/pub/flooding/output/{variable}/")
    input_dir = root / scenario / model

    new_covariate = f"{covariate}_{summary_statistic}"
    # Get all yearly NetCDF files
    netcdf_files = input_dir.glob(f"{new_covariate}_*.nc")

    for f in netcdf_files:
        f.unlink()
        print(f"❌ Removed: {f}")

def main(model: str, scenario: str, variant: str, variable: str, adjustment_num: int) -> None:
    """Runs individual steps in sequence."""
    create_yearly_summary_netcdf(model, scenario, variant, variable, adjustment_num)
    stack_yearly_netcdf(model, scenario, variable, adjustment_num)
    clean_up_yearly_netcdf_files(model, scenario, variable, adjustment_num)

# Run main function with parsed arguments
main(args.model, args.scenario, args.variant, args.variable, args.adjustment_num)
