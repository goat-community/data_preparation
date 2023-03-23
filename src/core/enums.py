from enum import Enum


class IfExistsType(str, Enum):
    """Enum for the if_exists parameter of the to_sql method of the DataFrame class."""

    append = "append"
    replace = "replace"
    fail = "fail"
    
    
class SaveGeocodedDataMethodType(str, Enum):
    """Enum for the if_exists parameter of the to_sql method of the DataFrame class."""

    create = "create"
    append = "append"
    extend = "extend"
        
class GeocoderOriginFormatType(str, Enum):
    """Enum for the file format of the geocoder files."""
    csv = "csv"
    sql = "sql"
    
class GeocoderResultSchema(str, Enum):
    """Allowed schemas for the geocoder results."""
    temporal = "temporal"
    
class TableDumpFormat(str, Enum):
    """Allowed schemas for the geocoder results."""
    sql = "sql"
    dump = "dump"
    
class BuildingClassificationColumnTypes(str, Enum):
    """Columns that can be classified by building preparation"""
    
    residential_status = "residential_status"
    building_levels_residential = "building_level_residential"
    
class BuildingClassificationTypes(str, Enum):
    """Geometry types for building classification"""
    attribute = "attribute"
    point = "point"
    polygon = "polygon"
    