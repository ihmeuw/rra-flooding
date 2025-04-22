import xarray as xr # type: ignore
import numpy as np # type: ignore
import glob
import os
from datetime import datetime, timedelta
import pandas as pd # type: ignore
from rra_flooding.data import FloodingData
from rra_flooding import constants as rfc
from rra_flooding.helper_functions import parse_yaml_dictionary, load_yaml_dictionary
from rra_tools.shell_tools import mkdir, touch # type: ignore
import argparse
from pathlib import Path
import yaml

parser = argparse.ArgumentParser(description="Calculate adjustment rasters for each model and variable.")

# Define arguments
parser.add_argument("--model", type=str, required=True, help="Climate model name")
parser.add_argument("--variable", type=str, required=True, help="Variable to process")
parser.add_argument("--variant", type=str, default="r1i1p1f1", help="Model variant identifier")
parser.add_argument("--model_root", type=str, default=rfc.MODEL_ROOT, help="Root of the model directory")
# Parse arguments
args = parser.parse_args()

model = args.model
variant = args.variant
model_root = args.model_root
variable = args.variable


def extract_time_series_memory_efficient(variable: str, model:str, model_root: str) -> xr.DataArray:
    # Create empty dataset to store all data
    floodingdata = FloodingData(model_root)
    YEARS = range(1970, 2015)
    scenario = "historical"

    combined_data = None
    
    # Process each raster brick (year)
    for year in YEARS:
 
        variable_ds = floodingdata.load_output(variable, scenario, model, year, variable_name = "base")

        variable_da = variable_ds["value"].values
        variable_da[variable_da < 0] = 0
        variable_ds["value"] = (('time', 'lat', 'lon'), variable_da)
        
        # Ensure time dimension exists and is properly labeled
        if combined_data is None:
            combined_data = variable_ds
        else:
            combined_data = xr.concat([combined_data, variable_ds], dim='time')
    
    return(combined_data)


def create_raster_summary(combined_data: xr.DataArray, variable: str, adjustment_num: int, model_root: str) -> None:
    # Create a summary raster based on the adjustment type for the variable
    floodingdata = FloodingData(model_root)
    scenario = "historical"

    variable_dict = parse_yaml_dictionary(variable, adjustment_num)
    adjusted_variable = variable_dict["adjusted_variable"]
    adjustment_type = variable_dict["adjustment_type"]

    if (adjustment_type == "shifted") | (adjustment_type == "weighted"):
        tmp_shift_type = variable_dict["shift_type"]
        if tmp_shift_type == "min":
            tmp_raster_summary = combined_data.min(dim="time", skipna=True)
        elif tmp_shift_type == "percentile":
            tmp_shift = variable_dict["shift"]
            tmp_raster_summary = combined_data.quantile(dim="time", q=tmp_shift, skipna=True)
        else:
            raise ValueError(f"Unknown shift type: {tmp_shift_type}")
        # Write the raster summary to a file
        path = floodingdata.stacked_output_path(
            variable, scenario, model, variable_name=f"{adjusted_variable}_raster_summary",
        )
        mkdir(path.parent, parents=True, exist_ok=True)
        touch(path, clobber=True)
        encoding = {
            "value": {"zlib": True, "complevel": 5, "dtype": "float32"},  # Apply compression to data variable # Call this value
            "lon": {"dtype": "float32", "zlib": True, "complevel": 5},  # Compress longitude
            "lat": {"dtype": "float32", "zlib": True, "complevel": 5},  # Compress latitude
        }

        tmp_raster_summary.to_netcdf(path, format="NETCDF4", engine="netcdf4", encoding=encoding)

    elif adjustment_type == "unadjusted":
        print("No adjustment needed for this variable.")
    else:
        raise ValueError(f"Unknown adjustment type: {adjustment_type}")

def main(variable: str, model: str, model_root: str) -> None:
    """Runs individual steps in sequence."""

    combined_data = extract_time_series_memory_efficient(variable, model, model_root)
    
    YAML_PATH = rfc.REPO_ROOT / "rra-flooding" / "src" / "rra_flooding" / "VARIABLE_DICT.yaml"
    VARIABLE_DICT = load_yaml_dictionary(YAML_PATH)
    num_adjustments = len(VARIABLE_DICT[variable])

    for adjustment_num in range(num_adjustments):
        create_raster_summary(combined_data, variable, adjustment_num, model_root)
        print(f"✅ Created summary raster of {model} for {variable} with adjustment number {adjustment_num}.")
    print(f"✅ Finished processing {variable} for model {model}.")

if __name__ == "__main__":
    main(args.variable, args.model, args.model_root)