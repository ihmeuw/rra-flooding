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
parser.add_argument("--year", type=str, required=True, help="year to process")
parser.add_argument("--variable", type=str, required=True, help="variable to process")
parser.add_argument("--adjustment_num", type=int, required=True, help="Which adjustment to apply")
# Parse arguments
args = parser.parse_args()

SCRIPT_ROOT = Path.cwd()
OUTPUT_ROOT = Path("/mnt/team/rapidresponse/pub/flooding/output/")


def parse_yaml_dictionary(variable: str, adjustment_num: str) -> dict:
    # Read YAML
    with open(SCRIPT_ROOT.parent / "VARIABLE_DICT.yaml", 'r') as f:
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
        "adjustment_type": entry['adjustment']['type']
    }

    if entry['adjustment']['type'] == "shifted":
        result["shift_type"] = entry['adjustment'].get("shift_type")
        result["shift"] = entry['adjustment'].get("shift")

    return result
    


# Define a function of model, sceanrio, and variant that loops over the years in that scenario, reads in the daily flooding fraction brick for that combination
# and by pixels through days, finds the smallest non-negative value for each pixel (and skips is all pixels are negative or nan).
# Then it subtracts that smallest value from all days for that pixel. Finally, it saves the standardized flooding fraction as a new NetCDF file.

def standardize_flooding_fraction(model: str, scenario: str, variant: str,  year: int, variable: str, adjustment_num: int):
    variable_dict = parse_yaml_dictionary(variable, adjustment_num)
    # parse the variable dictionary
    covariate = variable_dict["variable"]
    adjustment_type = variable_dict["adjustment_type"]

    if adjustment_type == "shifted":
        shift_type = variable_dict["shift_type"]
        shift = variable_dict["shift"]
        new_covariate = f"{covariate}_{adjustment_type}{shift}"
    else:
        new_covariate = f"{covariate}_{adjustment_type}"


    print(f"Standardizing flooding fraction for {model}, {scenario}, {variant}, {year}...")
    input_file = OUTPUT_ROOT / variable / scenario / model / f"{covariate}_{year}.nc"
    output_file = OUTPUT_ROOT / variable / scenario / model / f"{new_covariate}_{year}.nc"

    if not input_file.exists():
        print(f"Input file {input_file} does not exist. Skipping...")
        return

    if adjustment_type == "unadjusted":
        ds = xr.open_dataset(input_file)
        # rename the variable to the new name
        ds = ds.rename({covariate: new_covariate})
        ds.attrs["long_name"] = f"Unadjusted {variable}"

        touch(output_file, clobber=True, mode=0o775)
        ds.to_netcdf(output_file, format="NETCDF4", engine="netcdf4", encoding=encoding)

        return
    

    # Read the daily flooding fraction data
    ds = xr.open_dataset(input_file)
    da = ds[covariate].values  # shape: (days, lat, lon)
    
    # Set all negative values to NaN
    da[da < 0] = np.nan
    
    # Create a copy for standardization
    da_adjusted = da.copy()
    # Change the name of the variable in da_weighted
    
    # Get dimensions
    days, height, width = da.shape
    
    # Process each pixel (lat, lon) separately to handle all-NaN cases
    for y in range(height):
        for x in range(width):
            pixel_values = da[:, y, x]
            # Skip if all values are NaN
            valid_values = pixel_values[~np.isnan(pixel_values)]
            if len(valid_values) > 0:
                # Step 1: compute the percentile value
                percentile_value = np.percentile(valid_values, shift * 100)

                # Step 2: subtract the percentile
                shifted_values = pixel_values - percentile_value

                # Step 3: # Replace negative values with 0
                shifted_values[shifted_values < 0] = 0

                # Store result
                da_adjusted[:, y, x] = shifted_values

    # Save the standardized flooding fraction as a new NetCDF file
    ds[covariate] = (('time', 'lat', 'lon'), da_adjusted)
    ds = ds.rename({covariate: new_covariate})  
    ds.attrs["long_name"] = f"{adjustment_type} {covariate} {shift_type} {shift}"

    # Define compression and data type encoding
    encoding = {
        new_covariate: {"zlib": True, "complevel": 5, "dtype": "float32"},  # Apply compression to data variable
        "lon": {"dtype": "float32", "zlib": True, "complevel": 5},  # Compress longitude
        "lat": {"dtype": "float32", "zlib": True, "complevel": 5},  # Compress latitude
        "time": {"dtype": "int32", "zlib": True, "complevel": 5, "units": f"days since {year}-01-01"}  # Compress time
    }
    touch(output_file, clobber=True, mode=0o775)
    ds.to_netcdf(output_file, format="NETCDF4", engine="netcdf4", encoding=encoding)
    
    # Close the dataset to free up resources
    ds.close()

if __name__ == "__main__":
    # Call the function with the parsed arguments
    standardize_flooding_fraction(args.model, args.scenario, args.variant, args.year, args.variable, args.adjustment_num)

