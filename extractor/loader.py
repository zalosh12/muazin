import pathlib
import datetime

class Loader:
    def __init__(self,directory_path):
        self.directory_path = pathlib.Path(directory_path)

    def extract_meta_data(self):
        files = []
        for f in self.directory_path.iterdir():
            file_data = {}
            file_path = str(pathlib.PurePosixPath(f))
            file_name = f.name
            file_size = f.stat().st_size
            created_at = f.stat().st_ctime
            to_time = str(datetime.datetime.fromtimestamp(created_at))
            file_data["path"] = file_path
            file_data["file_size"] = file_size
            file_data["file_name"] = file_name
            file_data["created_at"] = to_time
            files.append(file_data)

        return files