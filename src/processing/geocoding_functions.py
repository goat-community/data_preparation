from geopy import geocoders
from geopy import Nominatim
from geojson import Point
import json
from postal.parser import parse_address
from src.other.utility_functions import file2df, gdf_conversion


class GeoAddress:
    def __init__(
        self, street=None, houseno=None, city=None, country=None, postcode=None
    ):
        self.street = street if street is not None else "addr:street"
        self.houseno = houseno if houseno is not None else ""
        self.city = city if city is not None else "addr:city"
        self.country = country if country is not None else "addr:country"
        self.postcode = postcode if postcode is not None else "addr:postcode"


def getValue(d, attr):
    if attr != "":
        for k, v in d.items():
            if isinstance(v, dict):
                return getValue(v, attr)
            elif ("{}".format(k)) == attr:
                try:
                    return "{}".format(v)
                except:
                    return ""
    else:
        return ""


# json should be FeatureCollection structured
# if geometry exists it willbe replaced
def addLocationOfAdressToJson(input_fpath, output_fpath, g_api_key, addr_class):
    g = geocoders.GoogleV3(api_key=g_api_key)
    osm = Nominatim(user_agent="goat_community")

    j_file = open(input_fpath, "r+", encoding="utf-8")
    data = json.load(j_file)

    for i in data["features"]:
        address = (
            getValue(i, addr_class.street)
            + " "
            + getValue(i, addr_class.houseno)
            + " "
            + getValue(i, addr_class.city)
            # + " "
            # + getValue(i, addr_class.country)
            + " "
            + getValue(i, addr_class.postcode)
        )
        try:
            location = g.geocode(address)
            g_point = Point((location.longitude, location.latitude))
            try:
                i.pop("geometry")
            except:
                pass
            i["geometry"] = g_point
        except:
            try:
                location = osm.geocode(address)
                print("osm")
                osm_point = Point((location.longitude, location.latitude))
                i["geometry"] = osm_point
            except:
                point_0 = Point((0, 0))
                i["geometry"] = point_0

    with open(output_fpath, "w") as j_file:
        json.dump(data, j_file)



# Function allows to deagregate column with address as a str in separate columns (housenumber, addr:street, addr:city, addr:postcode)
# * requires installed libary libpostal
def redefine_address(filename, address_column):

    df = file2df(filename)
    df["housenumber"] = None
    df["addr:street"] = None
    df["addr:city"] = None
    df["addr:postcode"] = None

    for i in df.index:
        print(i)
        df_row = df.iloc[i]
        address = df_row[address_column]

        try:
            result = parse_address(address)
            for r in result:
                if r[1] == "house_number":
                    df["housenumber"][i] = r[0]
                elif r[1] == "road":
                    df["addr:street"][i] = r[0].capitalize()
                elif r[1] == "city":
                    df["addr:city"][i] = r[0].capitalize()
                elif r[1] == "postcode":
                    df["addr:postcode"][i] = r[0]
        except:
            print(f"Deagregation failed for row number {i} with address: {address}")
            pass

    df = df.drop(columns={address_column})

    gdf_conversion(df, filename.split(".")[0] + "_upd", "GPKG")




