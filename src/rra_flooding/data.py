from pathlib import Path
import yaml # type: ignore
import numpy as np # type: ignore
import xarray as xr # type: ignore
from rra_flooding import constants as rfc
from rra_tools.shell_tools import mkdir, touch # type: ignore

class FloodingData:
    """
    Class to handle flooding data.
    """

    def __init__(self, root: str | Path = rfc.MODEL_ROOT):
        self._root = Path(root)

    @property
    def root(self) -> Path:
        """
        Returns the root path of the flooding data.
        """
        return self._root

    @property
    def logs(self) -> Path:
        """
        Returns the path to the logs directory.
        """
        return self.root / "logs"
    
    def log_dir(self, step_name: str) -> Path:
        return self.logs / step_name
    
    @property
    def cama_root(self) -> Path:
        """
        Returns the path to the CaMa-Flood root directory.
        """
        return self.root / "CaMa-Flood" / "cmf_v420_pkg"

    @property
    def cama_outputs(self) -> Path:
        """
        Returns the path to the CaMa-Flood output directory.
        """
        return self.cama_root / "out" # Later make this: self.root / "cama_outputs"

    def cama_output_path(self, model: str, scenario: str, variant: str, variable: str, batch_years: str, year: int | str) -> Path:
        """
        Returns the path to the CaMa-Flood output file for the given parameters.
        """
        path = self.cama_outputs / f"{model}_{scenario}_{variant}_{batch_years}" / f"{variable}{year}.bin"

        return path

    def load_cama_output(self, model: str, scenario: str, variant: str, variable: str, batch_years: str, year: int | str) -> Path:
        """
        Loads the CaMa-Flood output file for the given parameters.
        """
        path = self.cama_output_path(model, scenario, variant, variable, batch_years, year)
        return np.fromfile(path, dtype="<f4")

    @property
    def output(self) -> Path:
        """
        Returns the path to the output directory.
        """
        return self.root / "output"
    
    def output_path(self, variable: str, scenario: str, model: str, year: int | str, variable_name: str) -> Path:
        """
        Returns the path to the output directory for the given parameters.
        """
        path = self.output / variable / scenario / model / f"{variable_name}_{year}.nc"
        return path
    
    def save_output(self, ds: xr.Dataset, variable: str, scenario: str, model: str, year: int | str, variable_name: str) -> None:
        """
        Saves the output data to the specified path.
        """
        path = self.output_path(variable, scenario, model, year, variable_name)
        mkdir(path.parent, parents=True, exist_ok=True)
        touch(path, clobber=True)

        # Define compression and data type encoding
        encoding = {
            "value": {"zlib": True, "complevel": 5, "dtype": "float32"},  # Apply compression to data variable # Call this value
            "lon": {"dtype": "float32", "zlib": True, "complevel": 5},  # Compress longitude
            "lat": {"dtype": "float32", "zlib": True, "complevel": 5},  # Compress latitude
            "time": {"dtype": "int32", "zlib": True, "complevel": 5, "units": f"days since {year}-01-01"}  # Compress time
        }

        ds.to_netcdf(path, format="NETCDF4", engine="netcdf4", encoding=encoding)

    def load_output(self, variable: str, scenario: str, model: str, year: int | str, variable_name: str) -> xr.Dataset:
        """
        Loads the output data from the specified path.
        """
        path = self.output_path(variable, scenario, model, year, variable_name)
        ds = xr.open_dataset(path)
        return ds
    
    def stacked_output_path(self, variable: str, scenario: str, model: str, variable_name: str) -> Path:
        """
        Returns the path to the output directory for the given parameters.
        """
        path = self.output / variable / scenario / model / f"{variable_name}.nc"
        return path

    def save_stacked_output(self, ds: xr.Dataset, variable: str, scenario: str, model: str, variable_name: str) -> None:
        """
        Saves the output data to the specified path.
        """
        path = self.stacked_output_path(variable, scenario, model, variable_name)
        mkdir(path.parent, parents=True, exist_ok=True)
        touch(path, clobber=True)

        encoding = {var: {"zlib": True, "complevel": 5, "dtype": "float32"} for var in ds.data_vars}
        encoding.update({
            "time": {"dtype": "int32"},  # Remove "units" from encoding
            "lon": {"dtype": "float32", "zlib": True, "complevel": 5},
            "lat": {"dtype": "float32", "zlib": True, "complevel": 5},
        })

        ds.to_netcdf(path, format="NETCDF4", engine="netcdf4", encoding=encoding) 


# Almost certaintly shouldn't be here but :shrug:

    def parse_yaml_dictionary(variable: str, adjustment_num: str) -> dict:
        YAML_PATH = rfc.REPO_ROOT / "rra_flooding" / "cama" / "variable_dictionary.yaml"
        
        # Read YAML
        with open(YAML_PATH, 'r') as f:
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
            "adjustment_type": entry['adjustment']['type'],
            "summary_statistic": entry['summary_statistic']['type']
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

        result["summary_variable"] = f"{result['adjusted_variable']}_{result['summary_statistic']}"

        return result
