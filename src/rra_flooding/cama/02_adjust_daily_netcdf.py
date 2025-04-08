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
parser.add_argument("--year", type=str, required=True, help="year to process",)

# Parse arguments
args = parser.parse_args()

covariate = "flood_fraction"
new_covariate = "fldfrc_weighted"
OUTPUT_ROOT = Path("/mnt/team/rapidresponse/pub/flooding/output/fldfrc")


# Define a function of model, sceanrio, and variant that loops over the years in that scenario, reads in the daily flooding fraction brick for that combination
# and by pixels through days, finds the smallest non-negative value for each pixel (and skips is all pixels are negative or nan).
# Then it subtracts that smallest value from all days for that pixel. Finally, it saves the standardized flooding fraction as a new NetCDF file.

def standardize_flooding_fraction(model: str, scenario: str, variant: str, covariate: str, new_covariate: str, year: int):
    print(f"Standardizing flooding fraction for {model}, {scenario}, {variant}, {year}...")
    input_file = OUTPUT_ROOT / scenario / model / f"flood_fraction_{year}.nc"
    output_file = OUTPUT_ROOT / scenario / model / f"{new_covariate}_{year}.nc"

    if not input_file.exists():
        print(f"Input file {input_file} does not exist. Skipping...")
        return

    # Read the daily flooding fraction data
    ds = xr.open_dataset(input_file)
    da = ds[covariate].values  # shape: (days, lat, lon)
    
    # Set all negative values to NaN
    da[da < 0] = np.nan
    
    # Create a copy for standardization
    da_weighted = da.copy()
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
                min_value = np.min(valid_values)
                weight = 1 - min_value
                # Subtract minimum from all days for this pixel
                if weight > 0:
                    da_weighted[:, y, x] = (da[:, y, x] - min_value) / weight
                else:
                    da_weighted[:, y, x] = 0
                # Set negative values to zero
                negative_mask = da_weighted[:, y, x] < 0
                da_weighted[negative_mask, y, x] = 0

    # Save the standardized flooding fraction as a new NetCDF file
    ds[covariate] = (('time', 'lat', 'lon'), da_weighted)
    ds = ds.rename({covariate: new_covariate})  # Rename variable to "fldfrc_weighted"
    ds.attrs["long_name"] = "Weighted flooding fraction"

    # Define compression and data type encoding
    encoding = {
        new_covariate: {"zlib": True, "complevel": 5, "dtype": "float32"},  # Apply compression to data variable
        "lon": {"dtype": "float32", "zlib": True, "complevel": 5},  # Compress longitude
        "lat": {"dtype": "float32", "zlib": True, "complevel": 5},  # Compress latitude
        "time": {"dtype": "int32", "zlib": True, "complevel": 5, "units": f"days since {year}-01-01"}  # Compress time
    }
    ds.to_netcdf(output_file, format="NETCDF4", engine="netcdf4", encoding=encoding)
    
    print(f"Weighted flooding fraction saved to {output_file}")

    # Close the dataset to free up resources
    ds.close()

if __name__ == "__main__":
    # Call the function with the parsed arguments
    standardize_flooding_fraction(args.model, args.scenario, args.variant, covariate, new_covariate, args.year)