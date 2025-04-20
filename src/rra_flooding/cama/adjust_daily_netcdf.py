import numpy as np # type: ignore
import xarray as xr # type: ignore
import pandas as pd # type: ignore
from pathlib import Path
from rra_tools.shell_tools import mkdir, touch # type: ignore
from rra_flooding.data import FloodingData
from rra_flooding import constants as rfc
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

SCRIPT_ROOT = Path.cwd()
REPO_ROOT = Path(str(SCRIPT_ROOT).split("rra-flooding")[0] + "rra-flooding")

def parse_yaml_dictionary(variable: str, adjustment_num: str) -> dict:
    # Read YAML
    with open(REPO_ROOT / "src" / "rra_flooding" / "VARIABLE_DICT.yaml", 'r') as f:
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
        if entry['adjustment'].get("shift_type") == "percentile":
            result["shift"] = entry['adjustment'].get("shift")
            result["adjusted_variable"] = f"{variable}_{entry['adjustment']['type']}{entry['adjustment']['shift']}"
        elif entry['adjustment'].get("shift_type") == "min":
            result["adjusted_variable"] = f"{variable}_{entry['adjustment']['type']}min"
        else:
            raise ValueError(f"Unknown shift type: {entry['adjustment']['shift_type']}")
    elif entry['adjustment']['type'] == "unadjusted":
        result["adjusted_variable"] = f"{variable}_unadjusted"
    else:
        raise ValueError(f"Unknown adjustment type: {entry['adjustment']['type']}")

    return result
    
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
    # Read the daily flooding fraction data
    variable_da = variable_ds["value"].values  # shape: (days, lat, lon)
    # Set all negative values to NaN
    variable_da[variable_da < 0] = np.nan

    if adjustment_type == "unadjusted":
        # rename the variable to the new name
        variable_ds.attrs["long_name"] = f"Unadjusted {variable}"

        floodingdata.save_output(variable_ds, variable, scenario, model, year, variable_name = adjusted_variable)
        return
    
    elif adjustment_type == "shifted":
        shift_type = variable_dict["shift_type"]         

        # Create a copy for standardization
        variable_da_adjusted = variable_da.copy()
        # Change the name of the variable in da_weighted
        
        # Get dimensions
        days, height, width = variable_da.shape
        
        # Process each pixel (lat, lon) separately to handle all-NaN cases
        for y in range(height):
            for x in range(width):
                pixel_values = variable_da[:, y, x]
                # Skip if all values are NaN
                valid_values = pixel_values[~np.isnan(pixel_values)]
                if len(valid_values) > 0:
                    if shift_type == "percentile":
                        # Step 1: compute the percentile value
                        shift = variable_dict["shift"]
                        shift_value = np.percentile(valid_values, shift * 100)
                    elif shift_type == "min":
                        # Step 1: compute the minimum value
                        shift_value = np.min(valid_values)
                    else:
                        raise ValueError(f"Unknown shift type: {shift_type}")
                    
                    # Step 2: subtract the shift
                    shifted_values = pixel_values - shift_value

                    # Step 3: # Replace negative values with 0
                    shifted_values[shifted_values < 0] = 0

                    # Store result
                    variable_da_adjusted[:, y, x] = shifted_values

        # Save the standardized flooding fraction as a new NetCDF file
        variable_ds["value"] = (('time', 'lat', 'lon'), variable_da_adjusted)
        # Update the attributes
        if adjustment_type == "shifted":
            if shift_type == "percentile":
                variable_ds.attrs["long_name"] = f"{adjustment_type} {variable} {shift_type} {shift}"
            elif shift_type == "min":
                variable_ds.attrs["long_name"] = f"{adjustment_type} {variable} {shift_type}"

        floodingdata.save_output(variable_ds, variable, scenario, model, year, variable_name = adjusted_variable)
    else:
        raise ValueError(f"Unknown adjustment type: {adjustment_type}")

if __name__ == "__main__":
    # Call the function with the parsed arguments
    standardize_flooding_fraction(args.model, args.scenario, args.variant, args.year, args.variable, args.adjustment_num, args.model_root)