import yaml
from pathlib import Path
from rra_flooding.data import FloodingData
from rra_flooding import constants as rfc

# Is it better to nest these functions, have the material repeated or read both in every time. Most of the time we only need the second one
def load_yaml_dictionary(yaml_path: str) -> dict:
    # Read YAML
    with open(yaml_path, 'r') as f:
        yaml_data = yaml.safe_load(f)
    return(yaml_data['VARIABLE_DICT'])

def parse_yaml_dictionary(variable: str, adjustment_num: str) -> dict:
    YAML_PATH = rfc.REPO_ROOT / "rra-flooding" / "src" / "rra_flooding" / "VARIABLE_DICT.yaml"
    # Extract variable-specific config
    variable_dict = load_yaml_dictionary(YAML_PATH)
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