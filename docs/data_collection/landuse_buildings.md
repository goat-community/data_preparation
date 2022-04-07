# Landuse/Buildings
HowTo for Landuse and Building collection from OSM. 

## Landuse

In order to collect the data OSM related to land use you can do using the command line. If necessary, it is possible to adjust the setting in the **config.yaml** file of the section related to landuse. The configuration scheme is shown below. In section **collection** it is necessary specify the region(s) of data collection in **region_pbf**. In the **osm_tags** section it is necessary to specify the types of land use osm which are to be collected for GOAT. In the **additional_columns** you can specify the data from the OSM, which should be presented as separate columns.

Section **preparation/landuse_simplified** specifies new categories of land use and which OSM tags should be assigned to the new categories.

```yaml
  landuse:
    region_pbf : ["Mittelfranken", ...]
    collection:
      osm_tags:
        landuse: ["basin", "reservoir", ...] 
        amenity: ["parking", "school", "hospital"]
        leisure: ["adult_gaming_centre", "amusement_arcade", ...]
        natural: ["water", "scrub", "wood", "wetland", "grassland", "heath"]

      additional_columns: ["tourism", "name"]

      points    : True
      polygons  : True
      lines     : True

    preparation:
      landuse_simplified : 
        water          : ["basin","reservoir", ...]
        agriculture    : ["allotments", "aquaculture", ...]
        nature         : ["forest","grass", ...]
        leisure        : ["adult_gaming_centre", "amusement_arcade", ...]
        cemetery       : ["grave_yard"]
        residential    : ["garages"]
        commercial     : ["retail"]
        community      : ["school","university","hospital", ...]
        industrial     : ["landfill","quarry"]
        transportation : ["highway","parking","railway", "parking"]
        military       : []

    fusion: 
...  
```
Execution of the process is possible using the command line `collect.py -c landuse`. The result will be a table **landuse_osm** in the local database.

## Buildings

You can also use the command line to collect OSM data concerning buildings. As well as the landuse data, if necessary, it is possible to correct the collected data and define **additional_columns**. What you need to select are the data collection regions in **region_pbf**.

```yaml
...
  buildings :
    region_pbf: ["Mittelfranken", ...]
    collection:
      osm_tags: 
        building: [True]
      additional_columns : ["amenity", "leisure", "addr:street", ...]
      points: True
      polygons: True
      lines: True
    preparation:
    fusion:
...  
```
Execution of the process is possible using the command line `collect.py -c buildings`. The result will be a table **buildings_osm** in the local database.

