import os
import calendar
import numpy as np # type: ignore
import pandas as pd # type: ignore
import xarray as xr # type: ignore
from rra_tools.shell_tools import mkdir # type: ignore
from pathlib import Path
import argparse

VARIABLE = "fldfrc"
# Create the argument parser
parser = argparse.ArgumentParser(description="Generate daily netcdf bricks for each model and scenario.")

# Define arguments
parser.add_argument("--model", type=str, required=True, help="Climate model name")
parser.add_argument("--scenario", type=str, required=True, help="Climate scenario")
parser.add_argument("--start_year", type=int, required=True, help="Start year for processing")
parser.add_argument("--end_year", type=int, required=True, help="End year for processing")
parser.add_argument("--variant", type=str, default="r1i1p1f1", help="Model variant identifier")

# Parse arguments
args = parser.parse_args()

# Define paths
# OUTPUT_ROOT = Path("/mnt/team/rapidresponse/pub/flooding/scratch/netcdf") # TEST DIR
OUTPUT_ROOT = Path("/mnt/team/rapidresponse/pub/flooding/output/")
BIN_ROOT = Path("/mnt/team/rapidresponse/pub/flooding/CaMa-Flood/cmf_v420_pkg/out")

# Define the function inline
def create_netcdf_file(model: str, scenario: str, start_year: int, end_year: int, variant: str = "r1i1p1f1") -> None:
    batch_years = f"{start_year}-{end_year}"
    output_dir = OUTPUT_ROOT / VARIABLE / scenario / model
    mkdir(output_dir, parents=True, exist_ok=True)

    bin_name = f"{model}_{scenario}_{variant}_{batch_years}"
    bin_path = BIN_ROOT / bin_name

    # check if bin path exists, skip if not
    if not bin_path.exists():
        return
    
    # Define constants
    nodata = -9999
    covariate = "fldfrc"
    bin_file_var = "fldfrc"
    raster_width, raster_height = 1440, 720
    dtype = "<f4"

    # Precompute coordinates
    lon = np.linspace(-180, 180, raster_width)
    lat = np.linspace(90, -90, raster_height)


    # Loop through years
    for year in range(start_year, end_year + 1):

        # Time range
        days_in_year = 366 if calendar.isleap(year) else 365
        time_range = pd.date_range(f"{year}-01-01", periods=days_in_year)

        # Load binary data
        file_path = bin_path / f"{bin_file_var}{year}.bin"
        binary_data = np.fromfile(file_path, dtype=dtype)

        # Ensure correct shape
        expected_size = days_in_year * raster_height * raster_width
        if binary_data.size != expected_size:
            raise ValueError(f"File {file_path} size mismatch: {binary_data.size} vs {expected_size}")

        # Reshape and handle NaNs
        data_array = binary_data.reshape((days_in_year, raster_height, raster_width))
        data_array[data_array >= 1e20] = nodata  # Catch all large values
        data_array[np.isnan(data_array)] = nodata  # Catch NaNs explicitly


        # Create xarray Dataset
        ds = xr.Dataset(
            {covariate: (["time", "lat", "lon"], data_array)},
            coords={"lon": lon, "lat": lat, "time": time_range}
        )

        # Define compression and data type encoding
        encoding = {
            covariate: {"zlib": True, "complevel": 5, "dtype": "float32"},  # Apply compression to data variable
            "lon": {"dtype": "float32", "zlib": True, "complevel": 5},  # Compress longitude
            "lat": {"dtype": "float32", "zlib": True, "complevel": 5},  # Compress latitude
            "time": {"dtype": "int32", "zlib": True, "complevel": 5, "units": f"days since {year}-01-01"}  # Compress time
        }

        # Define output file path
        output_file = output_dir / f"{covariate}_{year}.nc"

        # Check if output file exists, if so, delete it
        if output_file.exists():
            output_file.unlink()

        # Save dataset to a compressed NetCDF file
        ds.to_netcdf(output_file, format="NETCDF4", engine="netcdf4", encoding=encoding)

        # Set file permissions to 775
        os.chmod(output_file, 0o775)


# Call the function with parsed arguments
create_netcdf_file(args.model, 
                   args.scenario, 
                   args.start_year, 
                   args.end_year, 
                   args.variant,
                   )