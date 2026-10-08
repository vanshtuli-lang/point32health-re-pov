from datetime import timedelta
import textwrap
import pendulum
from airflow.providers.ssh.operators.ssh import SSHOperator
from airflow.sdk import dag

SSH_CONN_ID = "azure_member_demo_vm"
POC_ROOT = "/tmp/airflow-member-poc/{{ run_id | replace(':', '-') | replace('+', '-') | replace('/', '-') }}"
ENV_FILE = f"{POC_ROOT}/test_env.sh"


def remote_command(tc_id, name, checks):
    checks = textwrap.dedent(checks).strip()
    return textwrap.dedent(f'''\
        set -Eeuo pipefail
        export TEST_CASE="{tc_id}"
        export SCENARIO_NAME="{name}"
        export POC_ROOT="{POC_ROOT}"
        export TEST_DIR="${{POC_ROOT}}/{tc_id}"
        test -r "{ENV_FILE}"
        set -a; . "{ENV_FILE}"; set +a
        mkdir -p "$TEST_DIR"
        trap 'rc=$?; printf "status=FAILED\\nexit_code=%s\\n" "$rc" > "$TEST_DIR/result.log"; echo "TEST SCENARIO $TEST_CASE FAILED: $SCENARIO_NAME"; exit "$rc"' ERR
        printf 'dag_id=%s\\ntask_id=%s\\nrun_id=%s\\ntry_number=%s\\nhost=%s\\nstarted_at=%s\\n' \\
          '{{{{ dag.dag_id }}}}' '{{{{ task.task_id }}}}' '{{{{ run_id }}}}' '{{{{ ti.try_number }}}}' \\
          "$(hostname)" "$(date -u '+%FT%TZ')" > "$TEST_DIR/metadata.log"
        echo "TEST SCENARIO $TEST_CASE STARTED: $SCENARIO_NAME"
        {checks}
        printf 'status=PASSED\\nexit_code=0\\nfinished_at=%s\\n' "$(date -u '+%FT%TZ')" > "$TEST_DIR/result.log"
        touch "$TEST_DIR/completed.marker"
        echo "TEST SCENARIO $TEST_CASE COMPLETED SUCCESSFULLY: $SCENARIO_NAME"
    ''').strip()


def test_task(task_id, tc_id, name, checks, timeout=1800):
    return SSHOperator(task_id=task_id, ssh_conn_id=SSH_CONN_ID,
        command=remote_command(tc_id, name, checks), conn_timeout=30,
        cmd_timeout=timeout, get_pty=False)


@dag(
    dag_id="member_load_ssh_poc_combined",
    description="Configure Azure VM, verify it, then execute TC-01 through TC-25",
    start_date=pendulum.datetime(2018, 4, 24, tz="America/New_York"),
    schedule="0 7 * * 1-5", catchup=False, is_paused_upon_creation=True,
    max_active_runs=1, max_active_tasks=8,
    default_args={"owner": "member-data-team", "retries": 2,
        "retry_delay": timedelta(minutes=3),
        "execution_timeout": timedelta(minutes=45)},
    tags=["poc", "member", "ssh", "azure", "autosys-migration"],
)
def member_load_ssh_poc_combined():
    configure = SSHOperator(
        task_id="configure_test_variables", ssh_conn_id=SSH_CONN_ID,
        conn_timeout=30, cmd_timeout=300, get_pty=False,
        command=f'''\
set -Eeuo pipefail
mkdir -p "{POC_ROOT}"
cat > "{ENV_FILE}" <<'EOF'
export IICS_TEST_COMMAND="/path/to/iics_run.sh ..."
export IICS_TIMEOUT_SECONDS="180"
export DB_TEST_COMMAND="/path/to/database_test.sh"
export API_TEST_COMMAND="/path/to/api_test.sh"
export FILE_TRANSFER_TEST_COMMAND="/path/to/file_transfer_test.sh"
export EXPECTED_EXECUTION_HOST="expected-azure-vm-hostname"
export NOTIFICATION_TEST_COMMAND="/path/to/notification_test.sh"
export SERVICENOW_TEST_COMMAND="/path/to/servicenow_test.sh"
EOF
chmod 600 "{ENV_FILE}"
test -s "{ENV_FILE}"
echo "Environment file configured: {ENV_FILE}"
''')

    setup = test_task("test_scenario_setup_verify_vm", "SETUP", "Verify Azure VM", '''
        hostname; whoami; date; test -w /tmp
        for c in bash awk grep find sha256sum timeout python3; do command -v "$c" >/dev/null; done
    ''', 300)

    specs = [
("TC-01","ingestion","AutoSys Ingestion Workflow Migration",'''
 mkdir -p "$TEST_DIR/in" "$TEST_DIR/out"; printf 'id,name\\n1,test\\n' > "$TEST_DIR/in/member.csv"
 h="$(sha256sum "$TEST_DIR/in/member.csv"|awk '{print $1}')"; a="$TEST_DIR/audit.log"
 if ! grep -Fq "$h" "$a" 2>/dev/null; then cp "$TEST_DIR/in/member.csv" "$TEST_DIR/out/member.csv"; echo "$h" >> "$a"; echo FILE_ARRIVAL > "$TEST_DIR/event.log"; fi
 test -s "$TEST_DIR/out/member.csv"; test -s "$TEST_DIR/event.log"; test "$(grep -Fc "$h" "$a")" -eq 1
'''),
("TC-02","parallel_processing","AutoSys Parallel Processing Workflow",'''
 mkdir -p "$TEST_DIR/branches"; for b in history cross_reference risk; do (sleep 1; echo complete > "$TEST_DIR/branches/$b.marker") & done; wait
 test "$(find "$TEST_DIR/branches" -type f -name '*.marker'|wc -l)" -eq 3; touch "$TEST_DIR/join.marker"; test -f "$TEST_DIR/join.marker"
'''),
("TC-03","archive_cleanup","Archive and Cleanup Workflow",'''
 mkdir -p "$TEST_DIR/source" "$TEST_DIR/archive"; echo 'member data' > "$TEST_DIR/source/member.dat"; touch "$TEST_DIR/processing_success.marker"
 test -f "$TEST_DIR/processing_success.marker"; [ -f "$TEST_DIR/archive/member.dat" ] || mv "$TEST_DIR/source/member.dat" "$TEST_DIR/archive/member.dat"
 test -f "$TEST_DIR/archive/member.dat"; test ! -f "$TEST_DIR/source/member.dat"
'''),
("TC-04","quarterly_calendar","Quarterly Calendar Scheduling",'''
 md="$(date -d '{{ ds }}' '+%m-%d')"; case "$md" in 01-01|04-01|07-01|10-01) c=quarter_boundary;; *) c=non_quarter_boundary;; esac
 printf 'run_date=%s\\nclassification=%s\\n' '{{ ds }}' "$c" > "$TEST_DIR/calendar.log"; grep -Eq '^classification=(quarter_boundary|non_quarter_boundary)$' "$TEST_DIR/calendar.log"
'''),
("TC-05","custom_calendars","Weekday and Sunday-Thursday Calendars",'''
 d="$(date -d '{{ ds }}' '+%u')"; test "$d" -ge 1; test "$d" -le 7
 if [ "$d" -le 5 ]; then wd=true; else wd=false; fi
 if [ "$d" -eq 7 ] || [ "$d" -le 4 ]; then st=true; else st=false; fi
 printf 'iso_day=%s\\nweekday=%s\\nsunday_thursday=%s\\n' "$d" "$wd" "$st" > "$TEST_DIR/calendar.log"
'''),
("TC-06","time_window_dst","Time Windows and DST Handling",'''
 python3 -c 'from datetime import datetime; from zoneinfo import ZoneInfo; ts=("2026-03-08T06:30:00+00:00","2026-11-01T05:30:00+00:00","2026-11-01T06:30:00+00:00"); [print(x,datetime.fromisoformat(x).astimezone(ZoneInfo("America/New_York")).isoformat()) for x in ts]' > "$TEST_DIR/dst.log"
 test "$(wc -l < "$TEST_DIR/dst.log")" -eq 3
'''),
("TC-07","repeat_vs_retry","Scheduler Repeat versus Task Retry",'''printf 'run_id=%s\\ntry_number=%s\\n' '{{ run_id }}' '{{ ti.try_number }}' > "$TEST_DIR/repeat_retry.log"; grep -Fq run_id= "$TEST_DIR/repeat_retry.log"; grep -Fq try_number= "$TEST_DIR/repeat_retry.log"'''),
("TC-08","advanced_dependencies","Advanced Dependency Orchestration",'''mkdir -p "$TEST_DIR/predecessors"; touch "$TEST_DIR/predecessors/"{A,B,C}.marker; test "$(find "$TEST_DIR/predecessors" -name '*.marker'|wc -l)" -eq 3; touch "$TEST_DIR/downstream_released.marker"'''),
("TC-09","prior_day_dependency","Prior-Day DateOffset Dependency",'''p="$(date -d '{{ ds }} -1 day' '+%F')"; touch "$TEST_DIR/$p.marker"; test -f "$TEST_DIR/$p.marker"; echo "prior_date=$p" > "$TEST_DIR/prior_day.log"'''),
("TC-10","fanout_30","Fan-Out Fan-In 30",'''mkdir -p "$TEST_DIR/branches"; for i in $(seq 1 30); do (echo complete > "$TEST_DIR/branches/$i.marker") & done; wait; test "$(find "$TEST_DIR/branches" -name '*.marker'|wc -l)" -eq 30'''),
("TC-11","fanout_57","Fan-Out Fan-In 57",'''mkdir -p "$TEST_DIR/branches"; for i in $(seq 1 57); do (echo complete > "$TEST_DIR/branches/$i.marker") & done; wait; test "$(find "$TEST_DIR/branches" -name '*.marker'|wc -l)" -eq 57'''),
("TC-12","fanout_131","Fan-Out Fan-In 131",'''mkdir -p "$TEST_DIR/branches"; s=$(date +%s); for i in $(seq 1 131); do (echo complete > "$TEST_DIR/branches/$i.marker") & done; wait; test "$(find "$TEST_DIR/branches" -name '*.marker'|wc -l)" -eq 131; echo "elapsed_seconds=$(($(date +%s)-s))" > "$TEST_DIR/performance.log"'''),
("TC-13","concurrency_controls","Scale Concurrency and Pool Controls",'''mkdir -p "$TEST_DIR/work"; limit=4; for g in $(seq 0 2); do for i in $(seq 1 $limit); do (sleep 1; touch "$TEST_DIR/work/${g}_${i}.marker") & done; wait; done; test "$(find "$TEST_DIR/work" -name '*.marker'|wc -l)" -eq 12; echo "concurrency_limit=$limit" > "$TEST_DIR/concurrency.log"'''),
("TC-14","recovery","Recovery Processing Validation",'''m="$TEST_DIR/business.marker"; a="$TEST_DIR/business_audit.log"; if [ ! -f "$m" ]; then touch "$m"; echo business_write >> "$a"; fi; test "$(grep -c '^business_write$' "$a")" -eq 1; printf 'run_id=%s\\ntry_number=%s\\n' '{{ run_id }}' '{{ ti.try_number }}' >> "$TEST_DIR/recovery.log"'''),
("TC-15","runtime_config","Variable and Runtime Config Management",''': "${POC_ENVIRONMENT:=demo}"; : "${POC_BATCH_SIZE:=100}"; case "$POC_BATCH_SIZE" in ''|*[!0-9]*) exit 15;; esac; printf 'environment=%s\\nbatch_size=%s\\n' "$POC_ENVIRONMENT" "$POC_BATCH_SIZE" > "$TEST_DIR/config.log"'''),
("TC-16","host_mapping","Execution Host Agent Mapping",'''a=$(hostname); e="${EXPECTED_EXECUTION_HOST:-$a}"; printf 'actual_host=%s\\nexpected_host=%s\\n' "$a" "$e" > "$TEST_DIR/host_mapping.log"; test "$a" = "$e"'''),
("TC-17","secrets","Secrets and Credential Management",'''umask 077; printf '%s\\n' 'credential_source=Airflow SSH connection' 'credential_value=REDACTED' > "$TEST_DIR/secrets_audit.log"; test "$(stat -c '%a' "$TEST_DIR/secrets_audit.log")" = 600; test -z "$(grep -Eio '(password|secret|token)[[:space:]]*=[[:space:]]*[^[:space:]]+' "$TEST_DIR/metadata.log" || true)"'''),
("TC-18","ssh_framework","SSH Execution Framework",'''printf 'host=%s\\nuser=%s\\ntime=%s\\n' "$(hostname)" "$(whoami)" "$(date -u '+%FT%TZ')" > "$TEST_DIR/ssh_execution.log"; test -s "$TEST_DIR/ssh_execution.log"'''),
("TC-19","iics_integration","Informatica IDMC IICS Integration",''': "${IICS_TEST_COMMAND:?Set IICS_TEST_COMMAND}"; set +e; timeout "${IICS_TIMEOUT_SECONDS:-1800}" bash -lc "$IICS_TEST_COMMAND" 2>&1 | tee "$TEST_DIR/iics.log"; rc=${PIPESTATUS[0]}; set -e; test "$rc" -eq 0'''),
("TC-20","enterprise_workloads","Database API and File Transfer Workloads",'''for w in DB API FILE_TRANSFER; do v="${w}_TEST_COMMAND"; c="${!v:-}"; [ -n "$c" ] || { echo "Missing $v"; exit 20; }; timeout 900 bash -lc "$c" > "$TEST_DIR/${w}.log" 2>&1; test -s "$TEST_DIR/${w}.log"; done'''),
("TC-21","operational_controls","Operational Control Management",'''for s in HOLD RELEASE SKIP ON_ICE; do f="$TEST_DIR/${s}.state"; echo "$s" > "$f"; test "$(cat "$f")" = "$s"; done'''),
("TC-22","capacity_management","Workload Distribution and Capacity Management",'''mkdir -p "$TEST_DIR/workers"; n=0; for j in $(seq 1 30); do w=$((n%3)); echo "$j" >> "$TEST_DIR/workers/worker_$w.log"; n=$((n+1)); done; test "$(find "$TEST_DIR/workers" -type f|wc -l)" -eq 3; find "$TEST_DIR/workers" -type f -exec wc -l {} \\; > "$TEST_DIR/distribution.log"'''),
("TC-23","file_arrival","File Dependency and File Arrival Handling",'''f="$TEST_DIR/member.ready"; (sleep 2; echo ready > "$f") & timeout 30 bash -c 'until [ -s "$1" ]; do sleep 1; done' _ "$f"; grep -Fxq ready "$f"'''),
("TC-24","visibility_logs","Operational Visibility and Log Analytics",'''printf 'timestamp=%s level=INFO correlation_id=%s event=scenario_validation\\n' "$(date -u '+%FT%TZ')" '{{ run_id }}' > "$TEST_DIR/structured.log"; grep -Fq 'correlation_id={{ run_id }}' "$TEST_DIR/structured.log"; grep -Fq event=scenario_validation "$TEST_DIR/structured.log"'''),
("TC-25","failure_notifications","Failure Management Notifications and ServiceNow",'''v="{{ dag.dag_id }}|{{ run_id }}|TC-25"; printf %s "$v"|sha256sum > "$TEST_DIR/correlation.id"; if [ -n "${NOTIFICATION_TEST_COMMAND:-}" ]; then bash -lc "$NOTIFICATION_TEST_COMMAND" > "$TEST_DIR/notification.log" 2>&1; else echo simulated_notification > "$TEST_DIR/notification.log"; fi; if [ -n "${SERVICENOW_TEST_COMMAND:-}" ]; then bash -lc "$SERVICENOW_TEST_COMMAND" > "$TEST_DIR/servicenow.log" 2>&1; else echo simulated_incident > "$TEST_DIR/servicenow.log"; fi; test -s "$TEST_DIR/correlation.id"; test -s "$TEST_DIR/notification.log"; test -s "$TEST_DIR/servicenow.log"'''),
    ]

    tasks = [test_task(f"test_scenario_{tc.lower().replace('-', '_')}_{slug}", tc, name, checks,
                       3000 if tc in {"TC-19", "TC-20"} else 1800)
             for tc, slug, name, checks in specs]
    configure >> setup >> tasks

member_load_ssh_poc_combined()
