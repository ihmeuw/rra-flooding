import getpass
import uuid
from jobmon.client.tool import Tool # type: ignore
from pathlib import Path
import geopandas as gpd # type: ignore
from rra_flooding import constants as rfc
from rra_flooding.helper_functions import load_yaml_dictionary

# Script directory
SCRIPT_ROOT = rfc.REPO_ROOT / "rra-flooding" / "src" / "rra_flooding" / "aggregate"
BASE_PATH = rfc.MODEL_ROOT / "output"
YAML_PATH = rfc.REPO_ROOT / "rra-flooding" / "src" / "rra_flooding" / "VARIABLE_DICT.yaml"


modeling_frame = gpd.read_parquet("/mnt/team/rapidresponse/pub/population-model/ihmepop_results/2025_03_22/modeling_frame.parquet")
block_keys = modeling_frame["block_key"].unique()
root = Path("/mnt/team/rapidresponse/pub/flooding/results/output/raw-results")

heirarchies = ["lsae_1209", "gbd_2021"]
# heirarchies = ["lsae_1209"]
models = ["ACCESS-CM2", "EC-Earth3", "INM-CM5-0", "MIROC6", "IPSL-CM6A-LR", "NorESM2-MM", "MRI-ESM2-0"]


VARIABLE_DICT = load_yaml_dictionary(YAML_PATH)

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
tool = Tool(name="flood_model")

# Create a workflow
workflow = tool.create_workflow(
    name=f"fld_pixel_workflow_{wf_uuid}",
    max_concurrently_running=10000,  # Adjust based on system capacity
)

# Compute resources
workflow.set_default_compute_resources_from_dict(
    cluster_name="slurm",
    dictionary={
        "memory": "15G",
        "cores": 1,
        "runtime": "60m",
        "queue": "all.q",
        "project": project,
        "stdout": str(stdout_dir),
        "stderr": str(stderr_dir),
    }
)

# Define the task template for processing each year batch
task_template = tool.get_task_template(
    template_name="fld_pixel_generation",
    default_cluster_name="slurm",
    default_compute_resources={
        "memory": "15G",
        "cores": 1,
        "runtime": "60m",
        "queue": "all.q",
        "project": project,
        "stdout": str(stdout_dir),
        "stderr": str(stderr_dir),
    },
    command_template=(
        "python {script_root}/pixel_main.py "
        "--hiearchy {{hiearchy}} "
        "--model {{model}} "
        "--block_key {{block_key}} "
        "--variable {{variable}} "
        "--adjustment_num {{adjustment_num}} "
    ).format(script_root=SCRIPT_ROOT),
    node_args=[ "hiearchy", "model", "block_key", "variable", "adjustment_num"],  # 👈 Include years in node_args
    task_args=[], # Only variation is task-specific
    op_args=[],
)


# Add tasks
tasks = []
for variable in VARIABLE_DICT.keys():
    num_adjustments = len(VARIABLE_DICT[variable])
    for i in range(num_adjustments):
        for hiearchy in heirarchies:
            for model in models:
                for block_key in block_keys:
                    # hier_model_block_file = root / hiearchy / model / block_key / "flood_fraction_sum_std" / "000.parquet"
                    # if hier_model_block_file.exists():
                    #     continue
                    tasks.append(
                        task_template.create_task(
                            hiearchy=hiearchy,
                            model=model,
                            block_key=block_key,
                            variable = variable,
                            adjustment_num=i 
                        )
                    )



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
