import os
import subprocess
from pathlib import Path

# Define the directory containing the shell scripts
GOSH_DIR = Path("/mnt/team/rapidresponse/pub/flooding/CaMa-Flood/cmf_v420_pkg/gosh")

def run_gosh_scripts(model: str, scenario: str, start_year: int, end_year: int, variation: str = "r1i1p1f1"):
    """
    Finds and executes all shell scripts matching the pattern:
    {model}_{scenario}_{variation}_{batch_years}.sh
    """
    # Get all .sh files matching the model, scenario, variation, and batch years
    script_pattern = f"{model}_{scenario}_{variation}_*.sh"
    scripts = sorted(GOSH_DIR.glob(script_pattern))

    if not scripts:
        print(f"⚠️ No matching scripts found for {model}, {scenario}, {variation}")
        return

    print(f"✅ Found {len(scripts)} scripts. Running them...")

    # Execute each shell script
    for script in scripts:
        print(f"🚀 Running: {script}")
        try:
            subprocess.run(["bash", str(script)], check=True)
        except subprocess.CalledProcessError as e:
            print(f"❌ Error executing {script}: {e}")

if __name__ == "__main__":
    # Example usage - replace with your actual model, scenario, etc.
    run_gosh_scripts("ACCESS-CM2", "ssp245", 2015, 2034)
