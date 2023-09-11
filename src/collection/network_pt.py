#TODO: Download the GTFS network from s3 and place into src/data/input/network_pt. We can locate the files per ID and read the regions from public.gtfs_regions
#TODO: Download the OSM data from Geofabrik and place into src/data/input/network_pt
#TODO: Split the network by region and save to src/data/output/network_pt as well as the s3 bucker 

#Notes: This in the end should be execute when running `python manage.py --actions=collection --region=eu --datasets=network_pt`` 