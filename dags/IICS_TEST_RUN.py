from datetime import timedelta

import pendulum
from airflow.providers.ssh.operators.ssh import SSHOperator
from airflow.sdk import dag


SSH_CONN_ID = "azure_member_demo_vm"

DEMO_DIR = (
    "/tmp/airflow-member-demo/"
    "{{ run_id | replace(':', '-') | replace('+', '-') }}"
)

# Configuration file created dynamically by the DAG.
INFA_ENV_FILE = f"{DEMO_DIR}/informatica_jobs.env"

IICS_SCRIPT = f"{DEMO_DIR}/iics_run.sh"
PMCMD_SCRIPT = f"{DEMO_DIR}/infa_pmcmd.sh"


@dag(
    dag_id="IICS_TEST_RUN_SCRIPT",
    description="Trigger IICS and CDI-PC jobs from Azure VM and retrieve logs",
    start_date=pendulum.datetime(
        2018,
        4,
        24,
        tz="America/New_York",
    ),
    schedule="0 7 * * 1-5",
    catchup=False,
    is_paused_upon_creation=True,
    max_active_runs=1,
    default_args={
        "owner": "member-data-team",
        "retries": 6,
        "retry_delay": timedelta(seconds=180),
    },
    tags=["demo", "member", "ssh", "azure", "iics", "cdipc"],
    doc_md=__doc__,
)
def member_load_ssh_demo():

    # ------------------------------------------------------------
    # 1. Verify Azure VM
    # ------------------------------------------------------------
    verify_vm = SSHOperator(
        task_id="verify_azure_vm",
        ssh_conn_id=SSH_CONN_ID,
        command="""
set -euo pipefail

echo "========================================"
echo " Azure VM verification"
echo "========================================"

echo "Hostname : $(hostname)"
echo "User     : $(whoami)"
echo "Date     : $(date)"
echo "OS       : $(uname -a)"
""",
    )

    # ------------------------------------------------------------
    # 2. Setup dummy Informatica configuration
    # ------------------------------------------------------------
    #
    # This task creates all values required by iics_run.sh and
    # infa_pmcmd.sh.
    #
    # Replace the dummy values with the real values when moving
    # from the migration/demo environment to production.
    # ------------------------------------------------------------
    ssetup_informatica_config = SSHOperator(
    task_id="setup_informatica_config",
    ssh_conn_id=SSH_CONN_ID,
    command=f"""
set -u -o pipefail

mkdir -p "{DEMO_DIR}/bin"

echo "========================================"
echo " Setting up Informatica test environment"
echo "========================================"

# ============================================================
# DUMMY IICS CLI
#
# Simulates Informatica cli.sh runAJobCli.
# Replace this with the real cli.sh path when DEMO_MODE=false.
# ============================================================

cat > "{DEMO_DIR}/bin/cli.sh" <<'IICS_DUMMY'
#!/usr/bin/env bash

set -u -o pipefail

echo "========================================"
echo " Dummy IICS CLI"
echo "========================================"

echo "Command received:"
echo "$*"

if [[ "$*" != *"runAJobCli"* ]]; then
    echo "ERROR: Expected runAJobCli"
    exit 2
fi

echo ""
echo "IICS Job"
echo "----------------------------------------"
echo "Task Name : dummy_member_taskflow"
echo "Task Type : TASKFLOW"
echo "Status    : RUNNING"

sleep 2

echo "Status    : SUCCESS"
echo "Job ID    : dummy-iics-job-001"

exit 0
IICS_DUMMY

chmod 700 "{DEMO_DIR}/bin/cli.sh"


# ============================================================
# DUMMY PMCMD
#
# Simulates Informatica pmcmd.
# ============================================================

cat > "{DEMO_DIR}/bin/pmcmd" <<'PMCMD_DUMMY'
#!/usr/bin/env bash

set -u -o pipefail

echo "========================================"
echo " Dummy Informatica pmcmd"
echo "========================================"

echo "Command received:"
echo "$*"

if [[ "$*" != *"startworkflow"* ]]; then
    echo "ERROR: Expected startworkflow"
    exit 2
fi

echo ""
echo "CDI-PC Workflow"
echo "----------------------------------------"
echo "Domain      : dummy_domain"
echo "Service     : dummy_integration_service"
echo "Folder      : dummy_folder"
echo "Workflow    : dummy_member_workflow"
echo "Status      : RUNNING"

sleep 2

echo "Status      : SUCCEEDED"
echo "Workflow ID : dummy-cdipc-workflow-001"

exit 0
PMCMD_DUMMY

chmod 700 "{DEMO_DIR}/bin/pmcmd"


# ============================================================
# Environment/configuration
# ============================================================

cat > "{INFA_ENV_FILE}" <<EOF
# ============================================================
# DEMO MODE
# ============================================================

DEMO_MODE=true


# ============================================================
# IICS / IDMC
# ============================================================

IICS_CLI_SH="{DEMO_DIR}/bin/cli.sh"

IICS_BASE_URL="https://dummy-iics.example.com/ma"
IICS_USERNAME="dummy_iics_user"
IICS_PASSWORD="dummy_iics_password"

IICS_TASK_KIND="TASKFLOW"
IICS_TASK_NAME="dummy_member_taskflow"

IICS_TASK_TYPE="DSS"
IICS_FOLDER_PATH="dummy_project/dummy_folder"


# ============================================================
# CDI-PC
# ============================================================

INFA_HOME="{DEMO_DIR}"

PMCMD_BIN="{DEMO_DIR}/bin/pmcmd"

INFA_DOMAIN="dummy_domain"
INFA_SERVICE="dummy_integration_service"

INFA_USER="dummy_cdipc_user"
INFA_PASSWORD="dummy_cdipc_password"

INFA_FOLDER="dummy_folder"
INFA_WORKFLOW="dummy_member_workflow"

INFA_DOMAINS_FILE="{DEMO_DIR}/domains.infa"

PATH="{DEMO_DIR}/bin:\$PATH"
LD_LIBRARY_PATH="{DEMO_DIR}/bin:\${{LD_LIBRARY_PATH:-}}"
EOF

chmod 600 "{INFA_ENV_FILE}"


echo ""
echo "========================================"
echo " Runtime validation"
echo "========================================"

echo ""
echo "IICS CLI:"
ls -l "{DEMO_DIR}/bin/cli.sh"

echo ""
echo "PMCMD:"
ls -l "{DEMO_DIR}/bin/pmcmd"

echo ""
echo "Configuration:"
grep -E '^[A-Z_]+=' "{INFA_ENV_FILE}" \
    | sed -E 's/(PASSWORD=).*/\\1********/'

echo ""
echo "========================================"
echo " Test environment ready"
echo "========================================"
""",
    cmd_timeout=60,
)

    # ------------------------------------------------------------
    # 3. Generate iics_run.sh and infa_pmcmd.sh
    # ------------------------------------------------------------
    create_informatica_scripts = SSHOperator(
        task_id="create_informatica_scripts",
        ssh_conn_id=SSH_CONN_ID,
        command=f"""
set -u -o pipefail

echo "========================================"
echo " Creating Informatica scripts"
echo "========================================"

if [ ! -f "{INFA_ENV_FILE}" ]; then
    echo "ERROR: Configuration file does not exist:"
    echo "{INFA_ENV_FILE}"
    exit 10
fi

# ============================================================
# iics_run.sh
# ============================================================

cat > "{IICS_SCRIPT}" <<'IICS_SCRIPT'
#!/usr/bin/env bash

set -u -o pipefail

ENV_FILE="__ENV_FILE__"

if [ ! -f "$ENV_FILE" ]; then
    echo "ERROR: Missing configuration file: $ENV_FILE"
    exit 10
fi

source "$ENV_FILE"

LOG_DIR="__DEMO_DIR__"
LOG_FILE="$LOG_DIR/iics_run.log"

mkdir -p "$LOG_DIR"

echo "========================================"
echo " IICS JOB START"
echo "========================================"

echo "Timestamp : $(date)"
echo "Task      : $IICS_TASK_NAME"

if [ "${{IICS_TASK_KIND:-TASKFLOW}}" = "TASKFLOW" ]; then

    "$IICS_CLI_SH" runAJobCli \
        -u "$IICS_USERNAME" \
        -p "$IICS_PASSWORD" \
        -bu "$IICS_BASE_URL" \
        -un "$IICS_TASK_NAME" \
        -t TASKFLOW \
        -w true \
        2>&1 | tee "$LOG_FILE"

else

    "$IICS_CLI_SH" runAJobCli \
        -u "$IICS_USERNAME" \
        -p "$IICS_PASSWORD" \
        -bu "$IICS_BASE_URL" \
        -t "$IICS_TASK_TYPE" \
        -n "$IICS_TASK_NAME" \
        -fp "$IICS_FOLDER_PATH" \
        -w true \
        2>&1 | tee "$LOG_FILE"

fi

EXIT_CODE="${{PIPESTATUS[0]}}"

echo ""
echo "========================================"
echo " IICS JOB END"
echo " Exit code: $EXIT_CODE"
echo "========================================"

exit "$EXIT_CODE"
IICS_SCRIPT

sed -i "s|__ENV_FILE__|{INFA_ENV_FILE}|g" "{IICS_SCRIPT}"
sed -i "s|__DEMO_DIR__|{DEMO_DIR}|g" "{IICS_SCRIPT}"

chmod 700 "{IICS_SCRIPT}"


# ============================================================
# infa_pmcmd.sh
# ============================================================

cat > "{PMCMD_SCRIPT}" <<'PMCMD_SCRIPT'
#!/usr/bin/env bash

set -euo pipefail

ENV_FILE="__ENV_FILE__"

if [ ! -f "$ENV_FILE" ]; then
    echo "ERROR: Missing configuration file: $ENV_FILE"
    exit 10
fi

source "$ENV_FILE"

LOG_DIR="__DEMO_DIR__"
LOG_FILE="$LOG_DIR/infa_pmcmd.log"

mkdir -p "$LOG_DIR"

echo "========================================"
echo " CDI-PC WORKFLOW START"
echo "========================================"

echo "Timestamp       : $(date)"
echo "Domain          : $INFA_DOMAIN"
echo "Integration Svc : $INFA_SERVICE"
echo "Folder          : $INFA_FOLDER"
echo "Workflow        : $INFA_WORKFLOW"

echo ""
echo "Starting CDI-PC workflow..."

"$PMCMD_BIN" startworkflow \
    -d "$INFA_DOMAIN" \
    -sv "$INFA_SERVICE" \
    -u "$INFA_USER" \
    -p "$INFA_PASSWORD" \
    -f "$INFA_FOLDER" \
    -wait \
    "$INFA_WORKFLOW" \
    2>&1 | tee "$LOG_FILE"

EXIT_CODE="${{PIPESTATUS[0]}}"

echo ""
echo "========================================"
echo " CDI-PC WORKFLOW END"
echo " Exit code: $EXIT_CODE"
echo "========================================"

exit "$EXIT_CODE"
PMCMD_SCRIPT

sed -i "s|__ENV_FILE__|{INFA_ENV_FILE}|g" "{PMCMD_SCRIPT}"
sed -i "s|__DEMO_DIR__|{DEMO_DIR}|g" "{PMCMD_SCRIPT}"

chmod 700 "{PMCMD_SCRIPT}"

echo ""
echo "Generated scripts:"
ls -l "{IICS_SCRIPT}" "{PMCMD_SCRIPT}"
""",
        cmd_timeout=60,
    )

    # ------------------------------------------------------------
    # 4. Execute IICS
    # ------------------------------------------------------------
    eexecute_iics = SSHOperator(
    task_id="execute_iics_job",
    ssh_conn_id=SSH_CONN_ID,
    command=f"""
set -u -o pipefail

echo "========================================"
echo " Executing iics_run.sh"
echo "========================================"

echo "Script:"
ls -l "{IICS_SCRIPT}"

echo ""
echo "IICS CLI:"
ls -l "{DEMO_DIR}/bin/cli.sh"

echo ""
echo "Running..."
echo ""

set +e
/bin/bash "{IICS_SCRIPT}"
EXIT_CODE=$?
set -e

echo ""
echo "========================================"
echo " IICS EXIT CODE: $EXIT_CODE"
echo "========================================"

exit $EXIT_CODE
""",
    cmd_timeout=6 * 60 ,
)

    # ------------------------------------------------------------
    # 5. Execute CDI-PC
    # ------------------------------------------------------------
    execute_pmcmd = SSHOperator(
    task_id="execute_cdipc_job",
    ssh_conn_id=SSH_CONN_ID,
    command=f"""
set -u -o pipefail

echo "========================================"
echo " Executing infa_pmcmd.sh"
echo "========================================"

echo "Script:"
ls -l "{PMCMD_SCRIPT}"

echo ""
echo "PMCMD:"
ls -l "{DEMO_DIR}/bin/pmcmd"

echo ""
echo "Running..."
echo ""

set +e
/bin/bash "{PMCMD_SCRIPT}"
EXIT_CODE=$?
set -e

echo ""
echo "========================================"
echo " CDI-PC EXIT CODE: $EXIT_CODE"
echo "========================================"

exit $EXIT_CODE
""",
    cmd_timeout=6 * 60 * 60,
)

    # ------------------------------------------------------------
    # 6. Return saved logs to Airflow
    # ------------------------------------------------------------
    collect_logs = SSHOperator(
        task_id="collect_informatica_logs",
        ssh_conn_id=SSH_CONN_ID,
        command=f"""
set -euo pipefail

echo "========================================"
echo " IICS LOG"
echo "========================================"

if [ -f "{DEMO_DIR}/iics_run.log" ]; then
    cat "{DEMO_DIR}/iics_run.log"
else
    echo "IICS log not found"
fi

echo ""
echo "========================================"
echo " CDI-PC / PMCMD LOG"
echo "========================================"

if [ -f "{DEMO_DIR}/infa_pmcmd.log" ]; then
    cat "{DEMO_DIR}/infa_pmcmd.log"
else
    echo "CDI-PC log not found"
fi

echo ""
echo "========================================"
echo " Log collection complete"
echo "========================================"
""",
        cmd_timeout=60,
    )

    # ------------------------------------------------------------
    # Dependencies
    # ------------------------------------------------------------
    verify_vm >> setup_informatica_config
    setup_informatica_config >> create_informatica_scripts

    create_informatica_scripts >> [
        execute_iics,
        execute_pmcmd,
    ]

    [
        execute_iics,
        execute_pmcmd,
    ] >> collect_logs


member_load_ssh_demo()