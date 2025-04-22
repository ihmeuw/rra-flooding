import numpy as np # type: ignore
import xarray as xr # type: ignore
import pandas as pd # type: ignore
from pathlib import Path
from rra_tools.shell_tools import mkdir, touch # type: ignore
from rra_flooding.data import FloodingData
from rra_flooding import constants as rfc
from rra_flooding.helper_functions import parse_yaml_dictionary
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
parser.add_argument("--model_root", type=str, default=rfc.MODEL_ROOT, help="Root of the model directory")

# Parse arguments
args = parser.parse_args()


    
def standardize_flooding_fraction(model: str, scenario: str, variant: str,  year: int, variable: str, adjustment_num: int, model_root: str):
    floodingdata = FloodingData(model_root)

    variable_dict = parse_yaml_dictionary(variable, adjustment_num)
    # parse the variable dictionary
    variable = variable_dict["variable"]
    adjustment_type = variable_dict["adjustment_type"]
    adjusted_variable = variable_dict["adjusted_variable"]    
    

    input_file_path = floodingdata.output_path(variable, scenario, model, year, variable_name = "base")
    if not input_file_path.exists():
        print(f"Input file {input_file_path} does not exist. Skipping...")
        return
    
    variable_ds = floodingdata.load_output(variable, scenario, model, year, variable_name = "base")

    variable_da = variable_ds["value"].values
    variable_da[variable_da < 0] = 0
    variable_ds["value"] = (('time', 'lat', 'lon'), variable_da)

    if adjustment_type == "unadjusted":
        # rename the variable to the new name
        variable_ds.attrs["long_name"] = f"Unadjusted {variable}"

        floodingdata.save_output(variable_ds, variable, scenario, model, year, variable_name = adjusted_variable)
        return
    
    elif (adjustment_type == "shifted") | (adjustment_type == "weighted"):
        shift_type = variable_dict["shift_type"]         

        # Read in the adjustment raster
        adjustment_raster_path = floodingdata.stacked_output_path(
            variable, "historical", model, variable_name=f"{adjusted_variable}_raster_summary",
        )

        # Load the adjustment raster
        adjustment_raster = xr.open_dataarray(adjustment_raster_path)
        adjustment_raster_values = adjustment_raster.values
        # Subtract the adjustment raster from the variable
        variable_da = variable_da - adjustment_raster_values
        if adjustment_type == "weighted":
            weight = 1 - adjustment_raster_values
            variable_da = variable_da / weight
            # Replace anywhere that weight was 0 with 0 in variable_da
            variable_da[weight == 0] = 0

        variable_da[variable_da < 0] = 0

        # Replace the variable in the dataset with the adjusted variable
        variable_ds["value"] = (('time', 'lat', 'lon'), variable_da)
        # Update the attributes
        if shift_type == "percentile":
            shift = variable_dict["shift"]
            variable_ds.attrs["long_name"] = f"{adjustment_type} {variable} {shift_type} {shift}"
        elif shift_type == "min":
            variable_ds.attrs["long_name"] = f"{adjustment_type} {variable} {shift_type}"

        floodingdata.save_output(variable_ds, variable, scenario, model, year, variable_name = adjusted_variable)
    else:
        raise ValueError(f"Unknown adjustment type: {adjustment_type}")

if __name__ == "__main__":
    # Call the function with the parsed arguments
    standardize_flooding_fraction(args.model, args.scenario, args.variant, args.year, args.variable, args.adjustment_num, args.model_root)