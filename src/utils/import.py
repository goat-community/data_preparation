import os 
from src.core.config import settings
from src.db.db import Database
import os
from src.utils.utils import print_info
import subprocess 



class ImportData:
    def __init__(self, db, dir: str):
        self.db = db
        self.dir = dir
        self.db_config = db.db_config

    def search_files_with_extension(extension: str, dir: str):
        file_paths = []

        # Traverse the directory structure using os.walk() function
        for dirpath, dirnames, filenames in os.walk(dir):
            # Loop through the filenames in the current directory
            for filename in filenames:
                # Check if the file has the extension .shp
                if filename.endswith(extension):
                    # If it does, append the full file path to the file_paths list
                    file_paths.append(os.path.join(dirpath, filename))

        return file_paths
    
    def import_ogr2ogr(self):
        
        files = self.search_files_with_extension('.shp', self.dir)
        cnt = 0 
        for f in files:
            if (not "geb" in f and f.endswith('_f'+'.shp')):
                if cnt == 0:
                    # Set up the ogr2ogr command
                    cmd = f'ogr2ogr -f "PostgreSQL" PG:"dbname={self.db_config.path[1:]} user={self.db_config.user} password={self.db_config.password}" -nln landuse {f} -overwrite'
                    # Run the command
                    subprocess.call(cmd, shell=True)
                else:
                    # append the data to the existing table
                    cmd = f'ogr2ogr -f "PostgreSQL" PG:"dbname={self.db_config.path[1:]} user={self.db_config.user} password={self.db_config.password}" -nln landuse {f} -append'
                    subprocess.call(cmd, shell=True)
                    
                print_info(f"Importing {f}")
                cnt = cnt + 1


def main():
    db = Database(settings.LOCAL_DATABASE_URI)
    import_data = ImportData(db, '/app/src/data/input/300001905_3266.basis-dlm-aaa_ebenen/basis-dlm-aaa_ebenen')
    import_data.import_ogr2ogr()


if __name__ == "__main__":
    main()