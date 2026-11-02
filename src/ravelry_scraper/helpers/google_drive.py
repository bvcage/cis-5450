import shutil
from pathlib import Path


def google_drive_paths():
    cloud_storage_dir = Path.home() / "Library" / "CloudStorage"

    if not cloud_storage_dir.exists(): return None # TODO

    drive_dirs = [
        dir for dir in cloud_storage_dir.iterdir()
        if dir.is_dir() and (
            "googledrive" in dir.name.lower() \
                or "google drive" in dir.name.lower()
        )
    ]
    
    return [Path(p).resolve() for p in set(drive_dirs) if p]


def google_drive_path(path: Path):
    target = path.resolve()
    
    for google_drive_dir in google_drive_paths():
        directory = google_drive_dir.resolve()

        if target.is_relative_to(directory):
            return True
        
    return False

def copy_to_google_drive_desktop(source_file: Path, destination_dir: Path):
    source_file = source_file.resolve()
    destination_dir = destination_dir.resolve()

    if not source_file.is_file(): return # TODO
    if not google_drive_path(destination_dir): return # TODO

    destination_dir.mkdir(parents=True, exist_ok=True)
    shutil.copy2(source_file, destination_dir)