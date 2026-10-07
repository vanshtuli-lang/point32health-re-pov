"""Claims file archive demo.

Uses the same SFTP server and file as `claims_sftp_to_oracle_parallel`:

1. Make two copies of the claims file next to the original.
2. Move copy 1 to /ftp/archive.
3. Zip the archived copy.
4. Grant the user delete access (demo: log only).
5. Delete the original file.
6. Revoke the user's delete access (demo: log only).

Copy 2 stays in the original location as a backup. Re-upload the original
file before re-running this DAG or the claims load DAGs.
"""

import io
import zipfile
from datetime import datetime

from airflow.providers.sftp.hooks.sftp import SFTPHook
from airflow.sdk import dag, task

SFTP_CONN_ID = "sftp_claims"
CLAIMS_FILE = "claims_data_6k.csv"
COPY_1 = "claims_data_6k_copy1.csv"
COPY_2 = "claims_data_6k_copy2.csv"
ARCHIVE_DIR = "/ftp/archive"
ARCHIVED_FILE = f"{ARCHIVE_DIR}/{COPY_1}"
ARCHIVED_ZIP = f"{ARCHIVE_DIR}/claims_data_6k_copy1.zip"
DEMO_USER = "claims_ops_user"


@dag(
    dag_id="claims_sftp_archive",
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
    tags=["demo", "claims", "sftp"],
    doc_md=__doc__,
)
def claims_sftp_archive():
    @task
    def make_copies():
        with SFTPHook(ssh_conn_id=SFTP_CONN_ID).get_conn() as sftp:
            with sftp.open(CLAIMS_FILE, "rb") as f:
                data = f.read()
            for copy in (COPY_1, COPY_2):
                with sftp.open(copy, "wb") as f:
                    f.write(data)
                print(f"Copied {CLAIMS_FILE} -> {copy} ({len(data)} bytes)")

    @task
    def move_copy_to_archive():
        with SFTPHook(ssh_conn_id=SFTP_CONN_ID).get_conn() as sftp:
            try:
                sftp.stat(ARCHIVE_DIR)
            except FileNotFoundError:
                sftp.mkdir(ARCHIVE_DIR)
            sftp.posix_rename(COPY_1, ARCHIVED_FILE)
        print(f"Moved {COPY_1} -> {ARCHIVED_FILE}")

    @task
    def zip_archived_file():
        with SFTPHook(ssh_conn_id=SFTP_CONN_ID).get_conn() as sftp:
            with sftp.open(ARCHIVED_FILE, "rb") as f:
                data = f.read()

            buffer = io.BytesIO()
            with zipfile.ZipFile(buffer, "w", zipfile.ZIP_DEFLATED) as zf:
                zf.writestr(COPY_1, data)

            with sftp.open(ARCHIVED_ZIP, "wb") as f:
                f.write(buffer.getvalue())
            sftp.remove(ARCHIVED_FILE)
        print(f"Zipped {ARCHIVED_FILE} -> {ARCHIVED_ZIP} ({len(data)} -> {buffer.tell()} bytes)")

    @task
    def grant_delete_access():
        print(f"Granting delete access on {CLAIMS_FILE} to user '{DEMO_USER}'")

    @task
    def delete_original():
        with SFTPHook(ssh_conn_id=SFTP_CONN_ID).get_conn() as sftp:
            sftp.remove(CLAIMS_FILE)
        print(f"Deleted original file {CLAIMS_FILE}")

    @task
    def revoke_delete_access():
        print(f"Revoking delete access on {CLAIMS_FILE} from user '{DEMO_USER}'")

    (
        make_copies()
        >> move_copy_to_archive()
        >> zip_archived_file()
        >> grant_delete_access()
        >> delete_original()
        >> revoke_delete_access()
    )


claims_sftp_archive()
