# POIs
HowTo for preparation of Points of Interest to GOAT database format.

Result of this operation will be local database table **pois_goat**. It will be neccessary to store result tables in remote database for execution of preparation for defined municiplaities.  
  
All settings for the subsequent preprocessing of the data are used by the file **_config.yaml_**. The **"pois"** section provides settings for **_"collection"_, _"preparation"_** and **_"fusion"_** in the corresponding categories. 
In the header of the configuration file there is an attribute _**"region_pbf"**_ in which as a list the regions for which the data collection and preparation will be performed. 
_!!! WARNING Be careful with the choice of region! Too large size and as a consequence, a large amount of data in the OSM for the region may significantly load the operating memory of the computer used and end in failure of the operation.!!!_  
## Collection/Preparation
### Collection
In the "collection" subsection, "osm_tags" is specified first. It specifies all kinds of points of interest to be collected from the OSM database. All tags should be correctly categorized by _"amenity", "shop",_ etc. according to the OSM documentation. When placing tags in the wrong category, selected points of interest will not be collected.  
There is an auxiliary function that allows you to automatically assign the desired tags to existing categories.   
The next sub-section contains the names of all attributes of the collected objects which will be converted into columns to be placed in the GOAT database. All attributes not specified here will be collected in the "tags" column in json format.  
The subsequent _"points", "polygons"_ and _"lines"_ subsections can be defined as True or False, determining which types of geometries will be collected from the OSM database. 
```yaml
VARIABLES_SET:
....
pois: 
    region_pbf : ["Mittelfranken", ...]
    collection:
      osm_tags:
        amenity : ["amenity_name1", "amenity_name2", ...]
        shop    : ["shop_name1", "shop_name2", ...]
        ...

      additional_columns: ["name", "brand", ...]
      points    : True # Have to be keeped as True
      polygons  : True # Have to be keeped as True
      lines     : True # Have to be keeped as True

```
### Preparation
The next subcategory **"preparation"** sets attributes for formatting and re-categorizing the data into the format used by the GOAT.  
All included subcategories allow the processing of raw OSM data by redefining the "operator" value (chain name) for POIs based on the POI category and name. 
In the following subcategories (_health_food, hypermarket, supermarket, discount_supermarket, no_end_consumer_store, discount_gym_) are the specific names of the network services and the possible "names" used in the OSM. The data is searched and the "amenity" values are changed according to the name of the subcategory. In addition to the points the name "operator" is set according to the defined value. For example: _"amenity"="supermarket", "operator"="rewe"_.
In the sections (organic, chemist, fast_food) the networks are defined in a similar way and the possible used names related to "shop" are redefined in the corresponding **"amenity"** and the value of the found "operator" is assigned.  
For "banks" only the **"operator"** is redefined according to the specified names in the configuration.  
There are 3 configuration lists in the **"sports"** subcategory. For all objects in the data is analyzed, if the defined values for "sport" or "leisure" is in leisure_var_add, but the value of "leisure" is not in the list leisure_var_disc and the value of "sport" is not in the list sport_var_disc, then the "amenity" for the object is set to "sport". All original "sport" and "leisure" values are stored in "tags".  
The last subcategory **"schools"** specifies the configuration for preparing a file with schools for subsequent fusion in the next step. The source file for the preparation from the service **"jedeschule"**. The analysis of the school names takes place and according to the matches the "amenity" schools are assigned according to the category name or are excluded if there is a match with the "exclude" list.  
```yaml
...
    preparation:
      bank: 
        sparkasse : ["kreissparkasse", "sparkasse", "stadtsparkasse"]
      ...
      supermarket: 
        rewe  : ["rewe", "rewe city"]
        ...
      sport:
        sport_var_disc   : [...]
        leisure_var_add  : [...]
        leisure_var_disc : [...]

      schools:
        schule: [...]
        grundschule: [...]
        hauptschule_mittelschule : [...]
        exclude: [...]
...
```
### Execution of Collection and Preparation
 - See chapter **Quick Start - Command Line**

### Manual execution of function for Collection
Collection can be done manually with function **_osm_collection()_** , where by default a dataframe and its name are returned as a tuple, which can subsequently be used for preparation function exported to the GOAT database. Variable **_conf_** should be defined by user. It should be **_pois_**, **_landuse_** or **_buildings_**. Variable **_database_** by default is local GOAT database, but can be specified manually. The result of the operation can be saved as a file in _crs/data/output_. For this purpose, you it is neccessary to specify variables **filename** and **return_type**. By default it is None.

### Manual execution of function for Preparation
Preparation of data also is possible to execute manually with the function **_pois_preparation()_**, where by default a dataframe and its name are returned as a tuple, and dataframe can subsequently be exported to the GOAT database. The function is able to accept the following variables (_config_, _config_buses_, _update_, _filename_, _return_type_). The variables **_config_** can be user-defined, but are defined inside the function by default. The result of the operation can be saved as a file in _crs/data/output_. For this purpose, you can use variables **filename** and **return_type**, in which you can specify file name and extension respectively. At the moment, the extensions **"GeoJSON"** and **"GPKG"** are supported, which can be defined in **return_type**. 

## Fusion
The fusion settings are in the corresponding subcategory. The fusion process uses data from remote database tables.  
To communicate with a remote database, you need to configure the connection configuration. For this purpose it is necessary to correct the file **db.yaml** in the folder **_src/config_**.  
The **_table_base_** specifies the name of the table with the POIs data prepared in the Collection/Prepare step.  
The table below shows the data format of the POIs table used in the GOAT. **!!! Fusion and replacement can only be done by the column names specified in the table.**

|Column name| Value |
|------------|------|
|addr:city|  City|
|addr:country| Country |
|addr:postcode| Zipcode |
|addr:street|  Street name|
|amenity| Amenity type |
|brand| Brand name|
|housenumber| House number
|name|Name of POI|
|opening_hours|Opening hours of POIs|
|operator|Operator name(chains)|
|origin_geometry|Origin geom in OSM|
|osm_id|ID in OSM|
|phone|Phonenumber|
|source|Source of data|
|tags|Additional information about POIs|
|website|Website|

The **_rs_set_** lists the rs codes of the desired areas. The data are extracted from the GeoNode, so it is assumed that there is a table **_"germany_municipalities"_** with the **_rs_** column in the database.  
Next comes a subcategory containing settings for data that will be integrated into the source file. At the moment, it is possible to use two resources for data this remote database (tables) and files format "GeoJSON" located locally (files should be placed in the folder _crs/data/input_). Conditionally, the data into two subcategories (**database** and **geojson**). The settings for the file have the same structure. The name of the settings set should coincide with the table name or file name (without extension). The configuration includes seven items: (**fusion_type**, **amenity**, **amenity_set**, **amenity_operator**, **columns2rename**, **column_set_value**, **columns2fuse**).
  
**fusion_type** - can be "replace" or "fuse". If set to **"replace"**, all already existing source data of the fusion **"amenity"** will be completely replaced by the data to be merged. If **"fuse"** is used, the original data will be merged with the custom data. If there is no location match, new points of interest will be added. 

**amenity** - here it is possible to specify the name of the amenity for which the fusion and replacement will be made. *If the replacement is by some trade network, then this setting should be left blank and the specification should be made in the section **amenity_operator**.  
  
**amenity_set** - It can be **True** or **False**. To accordingly set or not to set the amenity value in the returned data equal to the value specified in **"amenity "**. (It could be left blank or False if "amenity_operator" is used. Also if there is an amenity column in the data to be integrated where amenity values are specified. You have to specify the name of this column in **"columns2fuse"**). 

**amenity_operator** - here you specify the name of the amenity type and the name of the operator for which the fusion will be made as a tuple.  
  
**columns2rename** - in these settings you can rename columns in the input data if they are originally different from column names in **table_base**.  
  
**column_set_value** - allows to create a column and assign a pre-fusion value to it. For example, create a source column and specify the value.
  
**columns2fuse** - here should be specified the names of those columns in the input data whose contents will be integrated into the final table. 

**Important! "amenity", "operator" and "geometry" should not be defined in list, it will be attached automatically.**

```yaml
...
    fusion:
      table_base  : "pois_bayern"
      rs_set      : ["091620000","095640000", ...]
      fusion_data :
        source:
          database:
            doctors_bavaria:
              fusion_type : "replace" or "fuse"
              amenity : "amenity_name" # according OSM documentation
              amenity_set : False or True
              amenity_operator : ("amenity_name","operator_name") 
              columns2rename : {"old_column_name1" : "new_column_name1", "old_column_name2" : "new_column_name2", ...}
              column_set_value : {"new_column_name1" : "value1", "new_column_name2" : "value2", ...}
              columns2fuse : ["fuse_column_name1", "fuse_column_name2", ... ]
          geojson:
            dm:
              fusion_type : "replace" or "fuse"
              ...
            mueller:
              ...
...              
```
### Execution of Fusion
 - See chapter **Quick Start - Command Line**
 
### Manual execution of function
Fusion is done with a single function **pois_fusion()**, which by default returns the dataframe. It is possible to save the result of fusion as a file if the _result_filename_ and _return_type_ variables are defined. For example, **pois_fusion(df, config='pois', result_name="pois_result", return_type="GeoJSON")**. At the moment, the extensions **"GeoJSON"** and **"GPKG"** are supported. Result file will be saved in _crs/data/output_