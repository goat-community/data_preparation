import datetime
import glob
import json
import os
import subprocess

import boto3

from utils.utils import download_dir

s3_client = boto3.client(
    "s3",
    aws_access_key_id=os.environ["AWS_ACCESS_KEY_ID"],
    aws_secret_access_key=os.environ["AWS_SECRET_ACCESS_KEY"],
    region_name=os.environ["AWS_DEFAULT_REGION"],
)


class GTFS:
    def __init__(self):
        pass

    def clean(self, path, config):
        print(path, config)
        pass
    
    def run(self):
        data_dir = "./src/data"
        
        print("Download GTFS files from S3")
        download_dir("gtfs", data_dir, os.environ["AWS_BUCKET_NAME"], boto3=s3_client )
        with os.scandir(data_dir + "/gtfs") as it:
            for feed in it:
                if feed.name.endswith(".zip") and feed.is_file():
                    print("Unzipping " + feed.name)
                    # unzip the file
                    feed_name = feed.name[:-4]
                    subprocess.run(
                        f"unzip -o {data_dir}/gtfs/{feed.name} -d {data_dir}/gtfs/{feed_name}",
                        shell=True,
                        check=True,
                    )
                    
                    # clean the data
                    if os.path.exists(f"{data_dir}/gtfs/{feed_name}.json"):
                        print("Cleaning " + feed.name)
                        with open(f"{data_dir}/gtfs/{feed_name}.json") as f:
                            config = json.load(f)
                            self.clean(f"{data_dir}/gtfs/{feed_name}", config)
                            
                    # import to postgres
                    subprocess.run(
                        f"gtfs-via-postgres -s --trips-without-shape-id --schema gtfs_{feed_name} -- /app/src/data/gtfs/{feed_name}/*.txt | gzip >/app/src/data/gtfs/{feed_name}/{feed_name}.sql",
                        shell=True,
                        check=True,
                    )
                    
                    print(feed.name, feed.path)
        print("Load GTFS files into Postgres")


if __name__ == "__main__":
    gtfs = GTFS()
    gtfs.run()