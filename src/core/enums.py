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