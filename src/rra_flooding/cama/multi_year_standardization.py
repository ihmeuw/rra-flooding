import numpy as np # type: ignore
import xarray as xr # type: ignore
import pandas as pd # type: ignore
import cftime # type: ignore
from pathlib import Path
from rra_tools.shell_tools import mkdir # type: ignore
import os
import shutil
import re
import argparse

# Create the argument parser
parser = argparse.ArgumentParser(description="Run flooding model standardization for multiple years.")

# Define arguments
parser.add_argument("--model", type=str, required=True, help="Climate model name")
parser.add_argument("--scenario", type=str, required=True, help="Climate scenario")
parser.add_argument("--start_year", type=int, required=True, help="Start year for processing")
parser.add_argument("--end_year", type=int, required=True, help="End year for processing")
parser.add_argument("--variant", type=str, default="r1i1p1f1", help="Model variant identifier")

# gosh_template_path
#### FIX THIS TO BE NOT HARD CODED
gosh_template_path = Path("/mnt/share/homes/bcreiner/repos/rra-flooding/src/rra_flooding/cama/gosh_template.sh")

# Parse arguments
args = parser.parse_args()

def read_and_preprocess_raster(model: str, scenario: str, year: str | int, variant: str = "r1i1p1f1") -> xr.Dataset:
    """
    Read in the raster file, extract the mrro variable, and preprocess the data to a yearly dataset.
    """
    base_root = Path("/mnt/team/rapidresponse/pub/flooding/scratch/raw_data/esgf_metagrid")
    model = model
    scenario = f"scenario_{scenario}"
    variant = f"variant_{variant}"

    model_root = base_root / model / scenario / variant

    for file in model_root.iterdir():
        start_year = file.stem.split('_')[-1].split('-')[0][:4]
        end_year = file.stem.split('_')[-1].split('-')[1][:4]

        if start_year == end_year:
            if year == int(start_year):
                mrro_year = process_single_year_dataset(file, year)
        else:
            years = np.arange(int(start_year), int(end_year) + 1)
            if year in years:
                mrro_year = process_multi_year_dataset(file, year)

    return mrro_year

def process_single_year_dataset(file: str|Path, year: int) -> xr.Dataset:
    """
    Process a single year dataset.
    """
    mrro = xr.open_dataset(file, decode_times=True)
    mrro_year = mrro.sel(time=str(year))

    return mrro_year

def process_multi_year_dataset(file: str|Path, year: int) -> xr.Dataset:
    """
    Process a multi-year dataset by subsetting to the desired year.
    """

    mrro = xr.open_dataset(file, decode_times=True)
    mrro_year = mrro.sel(time=str(year))

    return mrro_year


def rescale_mrro_to_mm_day(mrro: xr.Dataset) -> xr.Dataset:
    """
    Rescale the mrro values in the raster by multiplying each value by 86400 to convert from kg m-2 s-1 to mm/day
    """
    
    mrro['Runoff'] = mrro['mrro'] * 86400
    # Remove the original mrro variable
    mrro = mrro.drop_vars('mrro')
    return mrro

def standardize_to_gregorian(ds: xr.Dataset) -> xr.Dataset:
    """
    Standardizes the dataset's time coordinate to the standard Gregorian calendar
    without including the time-of-day. This function converts non-standard calendars
    (like 'noleap') or numpy.datetime64 values to 'gregorian' by converting each time value
    to a cftime.DatetimeGregorian with hour, minute, and second set to 0.
    """
    # Extract original time values; these can be cftime objects or numpy.datetime64
    original_time = ds.time.values  
    
    # Create a new list of times in cftime.DatetimeGregorian format with time set to 00:00:00
    new_time_gregorian = []
    for t in original_time:
        # Check if t is already a cftime object (e.g., DatetimeNoLeap or DatetimeGregorian)
        if isinstance(t, (cftime.DatetimeNoLeap, cftime.DatetimeGregorian)):
            # Directly create a Gregorian object using t's date components
            new_time_gregorian.append(
                cftime.DatetimeGregorian(t.year, t.month, t.day, 0, 0, 0)
            )
        else:
            # Otherwise, convert t to a Python datetime using pandas
            dt = pd.to_datetime(t)
            new_time_gregorian.append(
                cftime.DatetimeGregorian(dt.year, dt.month, dt.day, 0, 0, 0)
            )
    
    # Assign the standardized Gregorian time coordinate back to the dataset
    ds = ds.assign_coords(time=new_time_gregorian)
    
    return ds

def check_leap_year_and_impute(ds: xr.Dataset, year: int) -> xr.Dataset:
    """
    Check if the dataset corresponds to a leap year and impute February 29 if missing.
    Works with cftime.DatetimeGregorian.
    """

    # Determine if the given year is a leap year
    is_leap_year = (year % 4 == 0 and year % 100 != 0) or (year % 400 == 0)
    
    if is_leap_year:

        # Extract existing time values
        time_values = ds.time.values  # These are cftime.DatetimeGregorian objects

        # Check if February 29 is already in the dataset
        leap_day = cftime.DatetimeGregorian(year, 2, 29)
        
        if not any(t == leap_day for t in time_values):

            # Select February 28 as the closest approximation
            feb_28 = ds.sel(time=cftime.DatetimeGregorian(year, 2, 28))
            
            # Create a new copy for February 29
            feb_29 = feb_28.copy()
            feb_29 = feb_29.assign_coords(time=[leap_day])

            # Concatenate the new data
            ds = xr.concat([ds, feb_29], dim='time')
            ds = ds.sortby('time')

    return ds

def center_align_longitude(ds: xr.Dataset) -> xr.Dataset:
    """
    Reorganize the interpolated dataset to go from -180 to 180 longitude.
    """
    ds = ds.assign_coords(lon=(ds.lon + 180) % 360 - 180)
    ds = ds.sortby('lon')

    return ds

def interpolate_to_1_degree_grid(ds: xr.Dataset) -> xr.Dataset:
    """
    Interpolate the mrro slices to a 1 degree grid.
    """
    ds_interp = ds.interp(
        lat=np.arange(-89.5, 90, 1), 
        lon=np.arange(-180, 180, 1), 
        time = ds.time,
        )
    # Remove the time, lat, and lon bnds variables if exist
    bands = ['time_bnds', 'lat_bnds', 'lon_bnds']
    for band in bands:
        if band in ds_interp:
            ds_interp = ds_interp.drop_vars(band)

    return ds_interp

def convert_to_little_endian_and_save(ds: np.ndarray, runoff_dir: str|Path, time_step: pd.Timestamp) -> None:
    """
    Convert the dataset to little-endian format and save it to a binary file.
    """
    year, month, day = time_step.strftime('%Y'), time_step.strftime('%m'), time_step.strftime('%d')
    output_file_path = runoff_dir / f"Roff____{year}{month}{day}.one"
    
    # convert to little-endian format
    ds = ds.astype('<f4')
    ds = np.flipud(ds)
    ds.tofile(output_file_path)
    
    # Set file permissions to 775
    os.chmod(output_file_path, 0o775)
    return
    
def extract_daily_data(ds: xr.Dataset, runoff_dir: str|Path = None) -> int:
    """
    Extract a specific time step from the dataset, converts time coordinates and returns a year, month, day string.
    """
    day_counter = 0
    for i in range(len(ds.time)):
        # Create a daily dataset
        ds_time_step = ds.isel(time=i)
        # Extract the time step
        time_step_values = ds_time_step.time.values.tolist()
        time_step = pd.Timestamp(time_step_values.year, time_step_values.month, time_step_values.day)
        # Convert xarray dataset to numpy array
        ds_time_step = ds_time_step.to_array().squeeze().data

        convert_to_little_endian_and_save(ds_time_step, runoff_dir, time_step)
        day_counter += 1
    
    return day_counter

def create_ctl_script(batch_dir: str|Path, start_year: int, day_counter: int) -> None:
    # create the ctl file
    ctl_file_path = batch_dir / "runoff.ctl"
    # write the ctl file
    with open(ctl_file_path, 'w') as f:
        f.write(f"dset  ^./runoff/Roff____%y4%m2%d2.one\n")
        f.write(f"undef -999\n")
        f.write(f"title \n")
        f.write(f"options yrev little_endian template\n")
        f.write(f"xdef  360 linear -180 1.0\n")
        f.write(f"ydef  180 linear  -89.5 1.0\n")
        f.write(f"tdef {day_counter} linear  00Z01jan{start_year} 1dy\n")
        f.write(f"zdef    1 linear  1 1\n")
        f.write(f"vars 1\n")
        f.write(f"var 0 99       ** runoff [mm/day]\n")
        f.write(f"ENDVARS\n")

    # Set file permissions to 775
    os.chmod(ctl_file_path, 0o775)
    
    return
    

def create_gosh_script(model: str, scenario: str, start_year: int, end_year: int, variant: str = "r1i1p1f1") -> None:
    """
    Create a gosh shell script for the CaMa-Flood model with 775 permissions.
    """

    # Read in the template script
    template_script = gosh_template_path
    batch_years = f"{start_year}-{end_year}"

    # Ensure the template script exists
    if not template_script.exists():
        raise FileNotFoundError(f"Template script not found: {template_script}")

    # Define the script directory and path
    script_dir = Path("/mnt/team/rapidresponse/pub/flooding/CaMa-Flood/cmf_v420_pkg/gosh")
    mkdir(script_dir, parents=True, exist_ok=True)

    # Define the script path
    script_path = script_dir / f"{model}_{scenario}_{variant}_{batch_years}.sh"

    # Copy the template script to the script directory
    shutil.copy(template_script, script_path)

    # Read the template script
    template = template_script.read_text()

    # Create placeholder replacements
    exp = f"{model}_{scenario}_{variant}_{batch_years}"
    crofdir = f"${{BASE}}/inp/{model}_{scenario}_{variant}_{batch_years}/runoff"
    cvarsout = "rivout,fldfrc,fldare,flddph"

    print(f"exp: {exp}, start_year: {start_year}, end_year: {end_year}, crofdir: {crofdir}, cvarsout: {cvarsout}")
    # Perform replacements in the template script
    template = re.sub(r'EXP=".*?"', f'EXP="{exp}"', template)
    template = re.sub(r'YSTA=".*?"', f'YSTA="{start_year}"', template)
    template = re.sub(r'YEND=".*?"', f'YEND="{end_year}"', template)
    template = re.sub(r'CROFDIR="\${BASE}/inp.*?"', f'CROFDIR="{crofdir}"', template)
    template = re.sub(r'CVARSOUT=".*?"', f'CVARSOUT="{cvarsout}"', template)

    # Write the new script
    script_path.write_text(template)

    # Set file permissions to 775
    os.chmod(script_path, 0o775)

    return


def make_directories(model: str, scenario: str, start_year: int, end_year: int, variant: str = "r1i1p1f1") -> list:
    """
    Create the necessary directories for the model standardization process.
    """
    root = Path("/mnt/team/rapidresponse/pub/flooding/CaMa-Flood/cmf_v420_pkg/inp/")
    # root = Path("/mnt/team/rapidresponse/pub/flooding/scratch/input/")  # TEST DIR

    # create batch year sub directories
    batch_years = f"{start_year}-{end_year}"
    batch_dir = root / f"{model}_{scenario}_{variant}_{batch_years}"
    # batch_dir = root / model / scenario / variant / batch_years
    mkdir(batch_dir, parents=True, exist_ok=True)

    runoff_dir = batch_dir / "runoff"
    mkdir(runoff_dir, parents=True, exist_ok=True)
    print(f"Runoff directory created: {runoff_dir}")

    return [batch_dir, runoff_dir]

def standardize_model(model: str, scenario: str, start_year: int, end_year: int, variant: str = "r1i1p1f1") -> None:
    """
    Run the flooding model standardization process for multiple years.
    """

    # root = Path("/mnt/team/rapidresponse/pub/flooding/CaMa-Flood/cmf_v420_pkg/inp/")
    batch_dir, runoff_dir = make_directories(model, scenario, start_year, end_year, variant)
    

    # Generate a list of years from start_year to end_year
    years = list(range(start_year, end_year + 1))

    total_days = 0
    for year in years:
        try:
            print(f"Processing {model}, {scenario}, {year}")
            # Only process the data if we have to
        
            # Step 1a: Read in raster file and determine how many years are present.
            mrro_year = read_and_preprocess_raster(model, scenario, year)
            # Step 2: Convert the mrro variable to mm/day.
            mrro_year = rescale_mrro_to_mm_day(mrro_year)
            # Step 3: Standardize the time coordinate to Gregorian and check for leap years.
            mrro_year = standardize_to_gregorian(mrro_year)
            mrro_year = check_leap_year_and_impute(mrro_year, year)
            # Step 4: Interpolate to a 1-degree grid and center align the longitude.
            mrro_year = center_align_longitude(mrro_year)
            mrro_year = interpolate_to_1_degree_grid(mrro_year)
        
            # Step 5: Extract a single time step and save the binary file.
            day_counter = extract_daily_data(mrro_year, process_data, runoff_dir)
            # Count total days processed
            total_days += day_counter

        except Exception as e:
            print(f"❌ Error processing {model}, {scenario}, {year}: {e}")
            continue  # Skips the failed year and moves to the next

    # Create the ctl file
    create_ctl_script(batch_dir, start_year, total_days)

    # Create the gosh script
    create_gosh_script(model, scenario, start_year, end_year, variant)

    print(f"Finished processing {model}, {scenario} for years {start_year}-{end_year} with {total_days} daily time steps.")
    return




# Call the function with parsed arguments
standardize_model(
    model=args.model,
    scenario=args.scenario,
    start_year=args.start_year,
    end_year=args.end_year,
    variant=args.variant
)


