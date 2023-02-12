from enum import Enum


class IfExistsType(str, Enum):
    """Enum for the if_exists parameter of the to_sql method of the DataFrame class."""

    append = "append"
    replace = "replace"
    fail = "fail"