import calendar
import numpy as np # type: ignore
import pandas as pd # type: ignore
import xarray as xr # type: ignore
from rra_tools.shell_tools import mkdir, touch # type: ignore
from pathlib import Path
from rra_flooding.data import FloodingData
from rra_flooding import constants as rfc
import argparse

# Create the argument parser
parser = argparse.ArgumentParser(description="Generate daily netcdf bricks for each model and scenario.")

# Define arguments
parser.add_argument("--model", type=str, required=True, help="Climate model name")
parser.add_argument("--scenario", type=str, required=True, help="Climate scenario")
parser.add_argument("--variable", type=str, required=True, help="Variable name to process")
parser.add_argument("--start_year", type=int, required=True, help="Start year for processing")
parser.add_argument("--end_year", type=int, required=True, help="End year for processing")
parser.add_argument("--variant", type=str, default="r1i1p1f1", help="Model variant identifier")
parser.add_argument("--model_root", type=str, default=rfc.MODEL_ROOT, help="Root of the model directory")

# Parse arguments
args = parser.parse_args()

# Define the function inline
def create_netcdf_file(model: str, scenario: str, variable: str, start_year: int, end_year: int, variant: str, model_root: str) -> None:
    floodingdata = FloodingData(model_root)
    
    batch_years = f"{start_year}-{end_year}"

    # Define constants
    nodata = -9999
    resolution = 0.25  # degrees
    # Precompute latitude and longitude arrays
    lon = np.arange(-180, 180, resolution)
    lat = np.arange(90, -90, -resolution)

    # Loop through years
    for year in range(start_year, end_year + 1):
        # Time range
        days_in_year = 366 if calendar.isleap(year) else 365
        time_range = pd.date_range(f"{year}-01-01", periods=days_in_year)

        # Load binary data
        binary_data = floodingdata.load_cama_output(model, scenario, variant, variable, batch_years, year)

        # Reshape and handle NaNs
        data_array = binary_data.reshape((days_in_year, len(lat), len(lon)))
        data_array[data_array >= 1e20] = nodata  # Catch all large values
        data_array[np.isnan(data_array)] = nodata  # Catch NaNs explicitly

        # Create xarray Dataset
        ds = xr.Dataset( # Have this be variable_ds
            {"value": (["time", "lat", "lon"], data_array)},
            coords={"lon": lon, "lat": lat, "time": time_range}
        )

        floodingdata.save_output(ds, variable, scenario, model, year, variable_name = "base")

# Call the function with parsed arguments
create_netcdf_file(args.model, 
                   args.scenario, 
                   args.variable,
                   args.start_year, 
                   args.end_year, 
                   args.variant,
                   args.model_root
                   )