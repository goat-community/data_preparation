from src.db.db import Database
from src.db.config import DATABASE_RD
from src.other.utils import (
    print_info,
    print_hashtags,
    print_warning,
)
import urllib.parse
import os
import argparse
import subprocess

#TODO: Implement with Argparse after further testing
def parse_args(args=None):
    # define the flags and their default values
    parser = argparse.ArgumentParser()
    parser.add_argument("--repo_url", required=True, help="URL of the repository")
    parser.add_argument("--maintainer", required=True, help="Name of the maintainer")
    parser.add_argument("--table_name", required=True, help="Name of the table")

    # parse the command line arguments
    return parser.parse_args(args)


class PrepareKart:
    """Clone Kart repo and setup with workingcopy in PostgreSQL
    """ 

    def __init__(self, db, repo_url: str, maintainer: str, table_name: str):
        """Initialize class

        Args:
            db (Database): Database object
            repo_url (str): URL of the repository
            maintainer (str): Name of the maintainer (PostgreSQL user)
            table_name (str): Name of the table
        """        
        self.db = db
        self.path_ssh_key = "/app/id_rsa"
        self.data_folder = "/app/src/data"

        # Prepare repository URL
        self.repo_url = repo_url
        parsed_url = urllib.parse.urlparse(self.repo_url)
        self.maintainer = maintainer
        self.table_name = table_name
        self.schema_name = f"kart_{table_name}_{maintainer}"

        # Get repo name and user name from URL
        self.repo_owner, self.repo_name = parsed_url.path.strip('/').split('/')
        self.git_domain = parsed_url.netloc
        self.repo_ssh_url = f'git@{self.git_domain}:{self.repo_owner}/{self.repo_name}'
        self.path_repo = os.path.join(self.data_folder, self.repo_name + "_" + self.maintainer)

    def clone_data_repo(self):
        """Clone Kart repository or pull recent changes
        """        

        # Check if folder exists
        if os.path.exists(self.path_repo):
            print_info(f"Folder {self.path_repo} already exists. Recent changes will be pulled.")
            # Open the repository
            os.chdir(self.path_repo)
            subprocess.run(
                f"kart pull",
                shell=True,
                check=True,
            )
        else: 
            # Clone repo
            print_info(f"Cloning repository {self.repo_ssh_url} to {self.path_repo}.")
            subprocess.run(
                f"kart clone {self.repo_ssh_url} {self.path_repo}",
                shell=True,
                check=True,
            ) 
        return 

    def create_schema(self):
        """Create empty kart schema in PostgreSQL and grant privileges to maintainer

        Raises:
            Exception: Schema already exists
            Exception: User does not exist (PostgreSQL user)
        """        
        sql_check_schema = f"""SELECT EXISTS (
            SELECT schema_name
            FROM information_schema.schemata
            WHERE schema_name = '{self.schema_name}'
        );"""

        sql_check_user = f"""SELECT EXISTS (
            SELECT usename
            FROM pg_catalog.pg_user
            WHERE usename = '{self.maintainer}'
        );"""

        # Check if user exists
        if not self.db.select(sql_check_user)[0][0]:
            raise Exception(f"User {self.maintainer} does not exist")

        # Check if schema exists
        if self.db.select(sql_check_schema)[0][0]:
            print_warning(
                f"Schema {self.schema_name} already exists. You want to delete it? (y/n)")
            if input() == "y":
                self.db.perform(f"DROP SCHEMA {self.schema_name} CASCADE")
                print_info(f"Schema {self.schema_name} deleted")
            else:
                raise Exception(f"You cannot initialize kart on an existing schema.")

        # Create schema
        sql_create_schema = f"""CREATE SCHEMA {self.schema_name};"""
        self.db.perform(sql_create_schema)
        print_info(f"Schema {self.schema_name} created")
        # Grant privilegescd
        sql_grant_privileges = f"""
            GRANT USAGE ON SCHEMA {self.schema_name} TO {self.maintainer};
            GRANT SELECT,INSERT,UPDATE,TRUNCATE,REFERENCES,TRIGGER 
            ON ALL TABLES IN SCHEMA {self.schema_name} TO {self.maintainer};
            GRANT USAGE, SELECT, UPDATE ON ALL SEQUENCES IN SCHEMA {self.schema_name} TO {self.maintainer};
            GRANT EXECUTE ON ALL FUNCTIONS IN SCHEMA {self.schema_name} TO {self.maintainer};
            ALTER DEFAULT PRIVILEGES IN SCHEMA {self.schema_name} GRANT SELECT,INSERT,UPDATE,TRUNCATE,REFERENCES,TRIGGER ON TABLES TO {self.maintainer};
            ALTER DEFAULT PRIVILEGES IN SCHEMA {self.schema_name} GRANT USAGE, SELECT, UPDATE ON SEQUENCES TO {self.maintainer};
            ALTER DEFAULT PRIVILEGES IN SCHEMA {self.schema_name} GRANT EXECUTE ON FUNCTIONS TO {self.maintainer};
        """
        self.db.perform(sql_grant_privileges)
        print_info(f"Privileges granted to {self.maintainer}")

    def kart_remote_workingcopy(self):
        """Create Kart remote working copy"""
        
        os.chdir(self.path_repo)
        # Execute command in command line to create Kart remote working copy
        subprocess.run(
            f'kart create-workingcopy postgresql://{self.db.db_config["user"]}:{self.db.db_config["password"]}@{self.db.db_config["host"]}/{self.db.db_config["dbname"]}/{self.schema_name}',
            shell=True,
            check=True,
        )
        sql_owner_kart_tables = f"""
            ALTER TABLE {self.schema_name}._kart_track OWNER TO {self.maintainer};
            ALTER TABLE {self.schema_name}._kart_state OWNER TO {self.maintainer};
        """
        self.db.perform(sql_owner_kart_tables)

    def prepare_schema_poi(self):
        """Prepare a database schema for POI"""
        
        sql_constraints_poi_category = f"""
           ALTER TABLE {self.schema_name}.poi_categories ADD CONSTRAINT poi_category_key UNIQUE (category);
           ALTER TABLE {self.schema_name}.poi_categories ALTER COLUMN category SET NOT NULL;
           ALTER TABLE {self.schema_name}.poi_categories OWNER TO {self.maintainer};
        """
        sql_constraints_data_source = f"""
            ALTER TABLE {self.schema_name}.data_source ADD CONSTRAINT data_source_key UNIQUE (name);
            ALTER TABLE {self.schema_name}.data_source ALTER COLUMN name SET NOT NULL;
            ALTER TABLE {self.schema_name}.data_source ALTER COLUMN url SET NOT NULL;
            ALTER TABLE {self.schema_name}.data_source OWNER TO {self.maintainer};
        """
        sql_constraints_poi_uid = f"""
            ALTER TABLE {self.schema_name}.poi_uid ALTER COLUMN uid SET NOT NULL;
            ALTER TABLE {self.schema_name}.poi_uid ALTER COLUMN category SET NOT NULL;
            ALTER TABLE {self.schema_name}.poi_uid ALTER COLUMN x_rounded SET NOT NULL;
            ALTER TABLE {self.schema_name}.poi_uid ALTER COLUMN y_rounded SET NOT NULL;
            ALTER TABLE {self.schema_name}.poi_uid ALTER COLUMN uid_count SET NOT NULL;
            ALTER TABLE {self.schema_name}.poi_uid ADD CONSTRAINT poi_uid_uid_key UNIQUE (uid);
            ALTER TABLE {self.schema_name}.poi_uid ADD FOREIGN KEY (category) REFERENCES {self.schema_name}.poi_categories(category);
            ALTER TABLE {self.schema_name}.poi_uid OWNER TO {self.maintainer};
        """
        sql_constraints_poi = f"""
            ALTER TABLE {self.schema_name}.poi ALTER COLUMN category SET NOT NULL;
            ALTER TABLE {self.schema_name}.poi ALTER COLUMN x_rounded SET NOT NULL;
            ALTER TABLE {self.schema_name}.poi ALTER COLUMN x_rounded SET DEFAULT 0;
            ALTER TABLE {self.schema_name}.poi ALTER COLUMN y_rounded SET NOT NULL;
            ALTER TABLE {self.schema_name}.poi ALTER COLUMN y_rounded SET DEFAULT 0;
            ALTER TABLE {self.schema_name}.poi ALTER COLUMN uid_count SET NOT NULL;
            ALTER TABLE {self.schema_name}.poi ALTER COLUMN uid_count SET DEFAULT 0;
            ALTER TABLE {self.schema_name}.poi ALTER COLUMN source SET NOT NULL;
            ALTER TABLE {self.schema_name}.poi ALTER COLUMN geom SET NOT NULL;
            ALTER TABLE {self.schema_name}.poi ALTER COLUMN edit_timestamp SET NOT NULL;
            ALTER TABLE {self.schema_name}.poi ALTER COLUMN edit_timestamp SET DEFAULT current_timestamp;
            ALTER TABLE {self.schema_name}.poi ALTER COLUMN edit_by SET NOT NULL;
            ALTER TABLE {self.schema_name}.poi ALTER COLUMN edit_by  SET DEFAULT current_user;
            ALTER TABLE {self.schema_name}.poi ADD CONSTRAINT other_uid_not_empty_string_check CHECK (other_uid != '');
            ALTER TABLE {self.schema_name}.poi ADD CONSTRAINT osm_type_check CHECK (osm_type IN ('w', 'n', 'r'));
            ALTER TABLE {self.schema_name}.poi ADD CONSTRAINT osm_id_and_osm_type_check1 CHECK ((osm_id IS NULL AND osm_type IS NOT NULL) = FALSE);
            ALTER TABLE {self.schema_name}.poi ADD CONSTRAINT osm_id_and_osm_type_check2 CHECK ((osm_id IS NOT NULL AND osm_type IS NULL) = FALSE);
            ALTER TABLE {self.schema_name}.poi ADD CONSTRAINT name_not_empty_string_check CHECK (name != '');
            ALTER TABLE {self.schema_name}.poi ADD CONSTRAINT operator_not_empty_string_check CHECK (operator != '');
            ALTER TABLE {self.schema_name}.poi ADD CONSTRAINT street_not_empty_string_check CHECK (street != '');
            ALTER TABLE {self.schema_name}.poi ADD CONSTRAINT housenumber_not_empty_string_check CHECK (housenumber != '');
            ALTER TABLE {self.schema_name}.poi ADD CONSTRAINT zipcode_not_empty_string_check CHECK (zipcode != '');
            ALTER TABLE {self.schema_name}.poi ADD CONSTRAINT phone_not_empty_string_check CHECK (phone != '');
            ALTER TABLE {self.schema_name}.poi ADD CONSTRAINT email_check CHECK (octet_length(email) BETWEEN 6 AND 320 AND email LIKE '_%@_%.__%');
            ALTER TABLE {self.schema_name}.poi ADD CONSTRAINT website_check CHECK (website ~* 'https?:\/\/(www\.)?[-a-zA-Z0-9@:%._\+~#=]{2,255}\.[a-z]{2,9}\y([-a-zA-Z0-9@:%_\+.~#?&//=]*)$' :: text);
            ALTER TABLE {self.schema_name}.poi ADD CONSTRAINT opening_hours_not_empty_string_check CHECK (opening_hours != '');
            ALTER TABLE {self.schema_name}.poi ADD CONSTRAINT wheelchair_check CHECK (wheelchair IN ('yes', 'no', 'limited'));
            ALTER TABLE {self.schema_name}.poi ADD CONSTRAINT uid_count_check CHECK (uid_count BETWEEN 0 AND 9999);
            ALTER TABLE {self.schema_name}.poi ADD CONSTRAINT tags_jsonb_check CHECK (jsonb_typeof(tags::jsonb) = 'object');
            ALTER TABLE {self.schema_name}.poi ADD CONSTRAINT poi_uid_key UNIQUE (uid);
            ALTER TABLE {self.schema_name}.poi ADD FOREIGN KEY (uid) REFERENCES {self.schema_name}.poi_uid(uid) ON DELETE CASCADE;
            ALTER TABLE {self.schema_name}.poi ADD FOREIGN KEY (category) REFERENCES {self.schema_name}.poi_categories(category) ON DELETE CASCADE;
            ALTER TABLE {self.schema_name}.poi OWNER TO {self.maintainer};
            """
        self.db.perform(sql_constraints_poi_category)
        self.db.perform(sql_constraints_data_source)
        self.db.perform(sql_constraints_poi_uid)
        self.db.perform(sql_constraints_poi)
        
        sql_create_trigger = f"""
            CREATE OR REPLACE FUNCTION {self.schema_name}.update_poi_trigger()
            RETURNS TRIGGER AS $$
            DECLARE 
                cnt integer;
                cnt_text TEXT; 
            BEGIN	
                NEW.edit_by = current_user;
                NEW.edit_timestamp = current_timestamp;
                NEW.x_rounded = (ST_X(NEW.geom) * 1000)::integer;
                NEW.y_rounded = (ST_Y(NEW.geom) * 1000)::integer;
            
                IF OLD.x_rounded = NEW.x_rounded AND OLD.y_rounded = NEW.y_rounded AND OLD.category = NEW.category THEN 
                    NEW.uid = OLD.uid; 
                ELSE 
                    cnt = (
                        SELECT CASE WHEN max(uid_count) IS NULL THEN 0 ELSE max(uid_count) + 1 END AS uid_count  
                        FROM {self.schema_name}.poi_uid p
                        WHERE p.x_rounded = NEW.x_rounded 
                        AND p.y_rounded = NEW.y_rounded 
                        AND p.category = NEW.category
                    ); 
                    NEW.uid_count = cnt; 
                
                    IF NEW.uid_count != 0 THEN 
                        cnt_text = REPLACE((NEW.uid_count::float / 1000)::TEXT, '.', '');
                    ELSE 
                        cnt_text = '0000';
                    END IF; 
                
                    NEW.uid = NEW.x_rounded::TEXT || '-' || NEW.y_rounded::TEXT || '-' || NEW.category || '-' || cnt_text; 
                    INSERT INTO {self.schema_name}.poi_uid(uid, category, x_rounded, y_rounded, uid_count)
                    SELECT NEW.uid, NEW.category, NEW.x_rounded, NEW.y_rounded, NEW.uid_count;
                    
                END IF; 
                
                IF NEW.uid IS NULL THEN 
                    RAISE 'UID cannot be NULL';
                END IF; 
            
                RETURN NEW;
            END;
            $$ LANGUAGE plpgsql;

            CREATE TRIGGER update_poi_trigger
            BEFORE UPDATE ON {self.schema_name}.poi
            FOR EACH ROW
            EXECUTE PROCEDURE {self.schema_name}.update_poi_trigger();

            CREATE TRIGGER insert_poi_trigger
            BEFORE INSERT ON {self.schema_name}.poi
            FOR EACH ROW
            EXECUTE PROCEDURE {self.schema_name}.update_poi_trigger();
        """
        self.db.perform(sql_create_trigger)
  
        
def main():
    # args = parse_args()
    # repo_url = args.repo_url
    # maintainer = args.maintainer
    # table_name = args.table_name
    
    print_hashtags()
    print_info("Start Prepare Kart")
    print_hashtags()
    # Get from user url of repo
    repo_url = input("Enter url of repository: ")
    # Get from user name of maintainer
    maintainer = input("Enter name of maintainer: ")
    # Get from user name of table
    supported_tables = ["poi"]
    table_name = input(f"""Enter one of the following table name "{','.join(supported_tables)}": """)
    if table_name not in supported_tables:
        raise Exception("Table name not supported")

    # Init db and class
    db = Database(DATABASE_RD)
    prepare_kart = PrepareKart(
        db, repo_url=repo_url, maintainer=maintainer, table_name=table_name)
    
    prepare_kart.clone_data_repo()
    prepare_kart.create_schema()
    prepare_kart.kart_remote_workingcopy()
    prepare_kart.prepare_schema_poi()

    print_hashtags()
    print_info("End Prepare Kart")
    print_hashtags()
    
if __name__ == "__main__":
    main()
