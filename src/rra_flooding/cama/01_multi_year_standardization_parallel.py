import getpass
import uuid
import pandas as pd # type: ignore
from jobmon.client.status_commands import workflow_tasks, task_status # type: ignore
from jobmon.client.tool import Tool # type: ignore
from pathlib import Path

# Script directory
SCRIPT_ROOT = Path.cwd()

# Flood Fraction Directory
BASE_PATH = Path('/mnt/team/rapidresponse/pub/flooding/output/fldfrc')
# Define models, scenarios, and years
MODELS = ["ACCESS-CM2", "EC-Earth3", "INM-CM5-0", "MIROC6", "IPSL-CM6A-LR", "NorESM2-MM", "GFDL-CM4", "MRI-ESM2-0"]
SCENARIOS = ["historical", "ssp126", "ssp245", "ssp585"]
# Batch of 5 years    
YEARS = {
    "historical1": (1970, 1974),
    "historical2": (1975, 1979),
    "historical3": (1980, 1984),
    "historical4": (1985, 1989),
    "historical5": (1990, 1994),
    "historical6": (1995, 1999),
    "historical7": (2000, 2004),
    "historical8": (2005, 2009),
    "historical9": (2010, 2014),
    "batch1": (2015, 2019),
    "batch2": (2020, 2024),
    "batch3": (2025, 2029),
    "batch4": (2030, 2034),
    "batch5": (2035, 2039),
    "batch6": (2040, 2044),
    "batch7": (2045, 2049),
    "batch8": (2050, 2054),
    "batch9": (2055, 2059),
    "batch10": (2060, 2064),
    "batch11": (2065, 2069),
    "batch12": (2070, 2074),
    "batch13": (2075, 2079),
    "batch14": (2080, 2084),
    "batch15": (2085, 2089),
    "batch16": (2090, 2094),
    "batch17": (2095, 2099),
    "batch18": (2100, 2100),
}


# Jobmon setup
user = getpass.getuser()

log_dir = Path(f"/mnt/share/homes/{user}/flood/")
log_dir.mkdir(parents=True, exist_ok=True)
# Create directories for stdout and stderr
stdout_dir = log_dir / "stdout"
stderr_dir = log_dir / "stderr"
stdout_dir.mkdir(parents=True, exist_ok=True)
stderr_dir.mkdir(parents=True, exist_ok=True)

# Project
project = "proj_lsae"  # Adjust this to your project name if needed

# create jobmon jobs
user = getpass.getuser()
wf_uuid = uuid.uuid4()

# Create a tool
tool = Tool(name="flood_model_standardization")


# Create a workflow, and set the executor
workflow = tool.create_workflow(
    name=f"flood_model_standardization_{wf_uuid}",
    max_concurrently_running = 500,
)

# Set resources on the workflow
workflow.set_default_compute_resources_from_dict(
    cluster_name="slurm",
    dictionary={
        "memory": "5G",
        "cores": 1,
        "runtime": "5m",
        "constraints": "archive",
        "queue": "all.q",
        "project": project,  # Ensure the project is set correctly
        "stdout": str(stdout_dir),
        "stderr": str(stderr_dir),
    }
)


# Define the task template for processing each year batch
task_template = tool.get_task_template(
    template_name="flood_model_standardization_task",
    default_cluster_name="slurm",
    default_compute_resources={
        "queue": "all.q",
        "cores": 1,
        "memory": "5G",
        "runtime": "5m",
        "queue": "all.q",
        "project": project,  # Ensure the project is set correctly
        "stdout": str(stdout_dir),
        "stderr": str(stderr_dir),
    },
    command_template=(
        "python {script_root}/multi_year_standardization.py "
        "--model {{model}} "
        "--scenario {{scenario}} "
        "--start_year {{start_year}} "
        "--end_year {{end_year}} "
        "--variant {{variant}}"
    ).format(script_root=SCRIPT_ROOT),
    node_args=["model", "scenario", "start_year", "end_year"],  # 👈 Include years in node_args
    task_args=["variant"],  # Only variant is task-specific
    op_args=[],
)

tasks = []
for model in MODELS:
    for scenario in SCENARIOS:
        scenario_dir = BASE_PATH / model / f"scenario_{scenario}"
        # if not scenario_dir.exists():
        #     continue
        if scenario == "historical":
            relevant_years = ["historical1", "historical2", "historical3", "historical4", "historical5", "historical6", "historical7", "historical8", "historical9"]
        else:
            relevant_years = ["batch1", "batch2", "batch3", "batch4", "batch5", "batch6", "batch7", "batch8", "batch9", "batch10", "batch11", 
                              "batch12", "batch13", "batch14", "batch15", "batch16", "batch17", "batch18"]

        for year_batch in relevant_years:
            start_year, end_year = YEARS[year_batch]
            task = task_template.create_task(
                model=model,
                scenario=scenario,
                start_year=start_year,
                end_year=end_year,
                variant="r1i1p1f1",
            )
            tasks.append(task)
    
print(f"Number of tasks: {len(tasks)}")

if tasks:
    workflow.add_tasks(tasks)
    print("✅ Tasks successfully added to workflow.")
else:
    print("⚠️ No tasks added to workflow. Check task generation.")

try:
    workflow.bind()
    print("✅ Workflow successfully bound.")
    print(f"Running workflow with ID {workflow.workflow_id}.")
    print("For full information see the Jobmon GUI:")
    print(f"https://jobmon-gui.ihme.washington.edu/#/workflow/{workflow.workflow_id}")
except Exception as e:
    print(f"❌ Workflow binding failed: {e}")

try:
    status = workflow.run()
    print(f"Workflow {workflow.workflow_id} completed with status {status}.")
except Exception as e:
    print(f"❌ Workflow submission failed: {e}")
