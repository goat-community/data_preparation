# Population/Buildings
HowTo for preparation of Population database tables generation. 

The process of calculating the population and generating the resulting tables requires a connection to a remote database. In addition, it is necessary for the remote database to have the necessary tables.

List of required tables:

|Table name| Value |
|------------|------|
|germany_municipalities_districts| Districts with population data |
|germany_buildings| Buildings data as alternative to OSM data |
|landuse_atkis| Landuse from Atkis |
|dlm250_polygon| Additional landuse data |
|urban_atlas| Urban atlas data |
|germany_grid_100_100| Census data |
|buildings_osm| Building data from OSM |
|landuse_osm| Landuse data from OSM |
|ways| Network table from OSM |
|planet_osm_point| Table with additional data from OSM (generated automatically with network collection) |
|pois_goat| POIs data collected and fused in collection phase |

If all of the above tables are available for desired study areas, the population calculation can be performed. Below are the settings for generating population points. They are located in the __config.yaml__ file in the __config__ folder and can be adjusted to user needs.

```yaml
...
  population:
    region_pbf: ["Mittelfranken", ...]
    collection:
    preparation:
      average_building_levels: '4'
      average_roof_levels: '1'
      average_height_per_level: '3.5'
      building_types_residential: ["apartments", "bungalow", ...]
      minimum_building_size_residential: '30'
      custom_landuse_no_residents: ["AX_TagebauGrubeSteinbruch", "AX_SportFreizeitUndErholungsflaeche", ...]
      custom_landuse_potential_residents: ["AX_FlaecheGemischterNutzung"]
      custom_landuse_additional_no_residents: ["Water", ...]
      custom_landuse_additional_potential_residents: ["Construction sites", ...]
      osm_landuse_no_residents: ["farmyard","construction", ...]
      tourism_no_residents: ["zoo"]
      amenity_no_residents: ["hospital","university", ...]
      one_meter_degree: '0.000009'
      compute_slope_impedance: "'no'"
      average_gross_living_area: '50'
      census_minimum_number_new_buildings: '1'
    fusion:
...              
```

The process can be called with command line `python prepare.py -p population`. Buildings data will be updated with custom data, custom data has priority. Population points will be attached to buildings entrances where it will be possible. During the classification of buildings as 'residential' and 'not residential' AI algorythm is used, so it is necessary to have __building_classifier_model.joblib__. It could be found in data folder and keeped in __input__ folder.
The result will be two tables (**population**, **building**) in the local database in __basic__ scheme.