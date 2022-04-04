# Network
HowTo for Network collection from OSM. 

Result of this operation will be local database tables **ways** and **ways_vertices_pgr**. It will be neccessary to store result tables in remote database for execution of preparation for defined municiplaities.  
  
All settings for the subsequent preprocessing of the data are used by the file **_config.yaml_**. The **"ways"** section provides settings for **_"collection"_, _"preparation"_** and **_"fusion"_** in the corresponding categories. 
In the header of the configuration file there is an attribute _**"region_pbf"**_ in which as a list the regions for which the data collection and preparation will be performed. 
_!!! WARNING Be careful with the choice of region! Too large size and as a consequence, a large amount of data in the OSM for the region may significantly load the operating memory of the computer used and end in failure of the operation.!!!_  
## Collection/Preparation
### Collection
In the **collection** block nothing should be specified. Data will be collected and stored in local GOAT database.
### Preparation
In the **preparation** block located variables which define network preparation process.
```yaml
VARIABLES_SET:
....
  ways:
    region_pbf: ["Freiburg"]
    collection: 
    preparation:
      excluded_class_id_walking: [0,101,102,103,104,105,106,107,501,502,503,504,701,801]
      excluded_class_id_cycling: [0,101,102,103,104,105,106,107,501,502,503,504,701,801]
      categories_no_foot: ["use_sidepath","no"]
      categories_no_bicycle: ["use_sidepath","no"]
      compute_slope_impedance: "'yes'"
      wheelchair:
        smoothness_no: ["very_bad","horrible","very_horrible","impassable"]
        smoothness_limited: ['bad']
        surface_no: ['ground','grass','sand','dirt','unhewn_cobblestone','unpaved']
        surface_limited: ['gravel']
        highway_onstreet_yes: ['living_street']
        highway_onstreet_limited: ['residential','service']
      categories_sidewalk_no_foot: ["separate"]
      cycling_surface:
        paving_stones: '0.2'
        sett: '0.3'
        unhewn_cobblestone: '0.3'
        cobblestone: '0.3'
        pebblestone: '0.3'
        unpaved: '0.2'
        compacted: '0.05'
        fine_gravel: '0.05'
        gravel: '0.3'
        sand: '0.4'
        grass: '0.25'
        mud: '0.4'
      one_meter_degree : 0.000009
    fusion:
```
### Execution of Collection and Preparation
 - See chapter **Quick Start - Command Line**

### Manual execution of function for Collection
Collection can be done manually with function **_network_collection()_** , which returns nothing, but stores collected data in local GOAT database. Variable **_conf_** by default is **ways** can be specified. Variable **_database_** by default is local GOAT database, but can be specified manually as well.

### Manual execution of function for Preparation
Preparation of data also is possible to execute manually with the creation of class enitity **_PrepareLayers('ways')_**. Then it is neccesary to execute function to created entity with function **_ways()_** which executes all preparation steps for network and stores it in table **ways** in local GOAT database.