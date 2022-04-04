# Repository structure

## Folders

### __docs__ 
Folder contains stucture of MkDocs documentation. 

### __src__
1. **collection**
Folder contains main scripts for collection data (except Network) from OSM Geofabrik, prepation and fusion it.
2. **config**
There is __config.yaml__ with configuration for collection, preparation and fusion of data. There are also files which creates **Config** class, stores libraries with possible OSM tags, mapconfig.xml, style template.
3. **data**
Contains three folders: __input__, __output__ and __temp__. For fusion it is necessary to store data in __input__ folder, which will be used during the fusion process. In folder __output__ are collected results from data processing. __temp__ folder is used for temporary files.
4. **db** 
Folder contains scripts, functions and classes related to local database preparation.
5. **export**
Contains instruments for export data from remote database.
6. **import**

7. **network**
In this folder stored instruments for network collection and preparation.
8. **other**
Folder contains utility functions for data convertion, migration between databases, dumping.
9. **population**
Contains scripts, function and classes for population preparation.
10. **processing**
There are function for geocoding and addresses normalization.