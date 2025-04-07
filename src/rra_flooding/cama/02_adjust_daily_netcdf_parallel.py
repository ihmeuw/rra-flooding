import getpass
import uuid
from jobmon.client.tool import Tool # type: ignore
from pathlib import Path

# Code directory
REPO_ROOT = Path.cwd()

# Flood Fraction Directory
BASE_PATH = Path('/mnt/team/rapidresponse/pub/flooding/output/fldfrc')
# Models, scenarios
MODELS = ["ACCESS-CM2", "EC-Earth3", "INM-CM5-0", "MIROC6", "IPSL-CM6A-LR", "NorESM2-MM", "GFDL-CM4", "MRI-ESM2-0"]
SCENARIOS = ["historical", "ssp126", "ssp245", "ssp585"]

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

wf_uuid = uuid.uuid4()
tool = Tool(name="daily_netcdf_brick_adjustment")

# Create a workflow
workflow = tool.create_workflow(
    name=f"daily_brick_adjustment_workflow_{wf_uuid}",
    max_concurrently_running=500,  # Adjust based on system capacity
)

# Compute resources
workflow.set_default_compute_resources_from_dict(
    cluster_name="slurm",
    dictionary={
        "memory": "50G",
        "cores": 2,
        "runtime": "10m",
        "queue": "all.q",
        "project": project,  # Ensure the project is set correctly
        "stdout": str(stdout_dir),
        "stderr": str(stderr_dir),
    }
)

# Define the task template for processing each year batch
task_template = tool.get_task_template(
    template_name="daily_brick_adjustment",
    default_cluster_name="slurm",
    default_compute_resources={
        "memory": "50G",
        "cores": 2,
        "runtime": "10m",
        "queue": "all.q",
        "project": project,  # Ensure the project is set correctly
        "stdout": str(stdout_dir),
        "stderr": str(stderr_dir),
    },
    command_template=(
        "python {repo_root}/02_adjust_daily_netcdf.py "
        "--model {{model}} "
        "--scenario {{scenario}} "
        "--variant {{variant}} "
        "--year {{year}} "
    ).format(repo_root=REPO_ROOT),
    node_args=["model", "scenario", "year"], 
    task_args=["variant"],  
    op_args=[],
)


# Add tasks
tasks = []
for scenario in SCENARIOS:
    for model in MODELS:
        fldfrc_root = BASE_PATH / scenario / model
        if not fldfrc_root.exists():
            print(f"Skipping {fldfrc_root}: does not exist")
            continue
        if scenario == "historical":
            start_year, end_year = 1970, 2014
        else:
            start_year, end_year = 2015, 2100
        for year in range(start_year, end_year + 1):
            # Check if the daily NetCDF file exists for the year
            input_file = fldfrc_root / f"flood_fraction_{year}.nc"
            if not input_file.exists():
                print(f"Skipping {input_file}: does not exist")
                continue
            # Create task for this model, scenario, and year
            task = task_template.create_task(
                model=model,
                scenario=scenario,
                variant="r1i1p1f1",
                year=year,  # Add year to task arguments
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
