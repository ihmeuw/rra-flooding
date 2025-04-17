import getpass
import uuid
from jobmon.client.tool import Tool # type: ignore
from pathlib import Path

# Script directory
SCRIPT_ROOT = Path.cwd()

BASE_PATH = Path('/mnt/team/rapidresponse/pub/flooding/output/')
MODELS = ["ACCESS-CM2", "EC-Earth3", "INM-CM5-0", "MIROC6", "IPSL-CM6A-LR", "NorESM2-MM", "GFDL-CM4", "MRI-ESM2-0"]
SCENARIOS = ["historical", "ssp126", "ssp245", "ssp585"]
VARIABLE_DICT = {
    "rivout": ["unadjusted", "max", 0],
    "fldfrc": ["shifted10", "sum", 0],
    "fldare": ["unadjusted", "mean", 0],
    "flddph": ["unadjusted", "countoverthreshold", 1.0]
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

wf_uuid = uuid.uuid4()
tool = Tool(name="daily_netcdf_brick_adjustment")

# Create a workflow
workflow = tool.create_workflow(
    name=f"yearly_brick_workflow_{wf_uuid}",
    max_concurrently_running=500,  # Adjust based on system capacity
)

# Compute resources
workflow.set_default_compute_resources_from_dict(
    cluster_name="slurm",
    dictionary={
        "memory": "50G",
        "cores": 2,
        "runtime": "120m",
        "queue": "all.q",
        "project": project,  # Ensure the project is set correctly
        "stdout": str(stdout_dir),
        "stderr": str(stderr_dir),
    }
)

# Define the task template for processing each year batch
task_template = tool.get_task_template(
    template_name="yearly_brick_generation",
    default_cluster_name="slurm",
    default_compute_resources={
        "memory": "50G",
        "cores": 2,
        "runtime": "120m",
        "queue": "all.q",
        "project": project,  # Ensure the project is set correctly
        "stdout": str(stdout_dir),
        "stderr": str(stderr_dir),
    },
    command_template=(
        "python  {script_root}/generate_yearly_summary_netcdf_bricks.py "
        "--variable {{variable}} "
        "--model {{model}} "
        "--scenario {{scenario}} "
        "--summary_statistic {{summary_statistic}} "
        "--threshold {{threshold}} "
        "--variant {{variant}}"
    ).format(script_root=SCRIPT_ROOT),
    node_args=["variable", "model", "scenario", "summary_statistic"], 
    task_args=["threshold", "variant"],  
    op_args=[],
)

# Add tasks
tasks = []
for variable in VARIABLE_DICT.keys():
    new_covariate, summary_statistic, threshold = VARIABLE_DICT[variable]
    for scenario in SCENARIOS:
        for model in MODELS:
            base_root = BASE_PATH / variable / scenario / model
            if not base_root.exists():
                continue
            task = task_template.create_task(
                model=model,
                scenario=scenario,
                variable=new_covariate,
                summary_statistic=summary_statistic,
                threshold=threshold,
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
