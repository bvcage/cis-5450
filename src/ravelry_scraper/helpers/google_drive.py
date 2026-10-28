import shutil
from pathlib import path


def google_drive_path(path: Path) -> bool:
    return True
    # drive_roots = []

    # if os.name == "nt": 
    #     drive_dir = Path(os.environ.get("LOCALAPPDATA", "")) / "Google/DriveFS"
    # else:
    #     drive_dir = Path.home() / "Library/Application Support/Google/DriveFS"

    # if not drive_dir.exists(): return False

    # google_drive_db_patterns = [
    #     "root_preference_sqlite.db",
    #     "*/root_preference_sqlite.db",
    #     "*/metadata_sqlite.db",
    # ]

    # for pattern in google_drive_db_patterns:
    #     for db_path in drive_dir.glob(pattern):
    #         try:
    #             conn = sqlite3.connect(f"file:{db_path}?mode=ro", uri=True, timeout=1.0)
    #             cursor = conn.cursor()

    #             try:
    #                 cursor.execute("SELECT local_path FROM root_preferences WHERE status = 1")
    #                 paths = [row[0] for row in cursor.fetchall() if row[0]]
    #                 drive_roots.extend(paths)
    #             except sqlite3.OperationalError:
    #                 pass  # Schema doesn't match this database type, skip to next check

    #             conn.close()
    #         except Exception:
    #             continue

    # # Clean and resolve path objects
    # return [Path(p).resolve() for p in set(drive_roots) if p]


def copy_to_google_drive_desktop(source_file: Path, destination_dir: Path):
    if not source_path.is_file(): return # TODO

    if not google_drive_path(destination_dir): return # TODO

    destination_dir.mkdir(parents=True, exist_ok=True)
    shutil.copy2(source_file, destination_dir)