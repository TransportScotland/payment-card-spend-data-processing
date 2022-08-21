print("This doesn't seem to work. Instead use nodejs mapshaper with the following command:")
print("mapshaper -i encoding=utf-8 path/to/infile.shp -simplify dp 2% keep-shapes -proj wgs84 -o format=geojson outfile.geojson")
# print("(This also simplifies the polygons to reduce file size, use the following to create full big files:")
# print("mapshaper -i encoding=utf-8 path/to/infile.shp -proj wgs84 -o format=geojson outfile.geojson")

# import sys
# if not( 2 <= len(sys.argv) <= 3): 
#     raise ValueError('invalid number of arguments. Use: python3 shp_to_geojson.py infile.shp outfile.json')
# infname = sys.argv[1]
# outfname = sys.argv[2] if len(sys.argv) == 3 else f"{infname.rsplit('.', 1)[0]}.json"

# # https://gis.stackexchange.com/a/279495
# import geopandas as gpd
# file = gpd.read_file(infname)
# file.to_file(outfname, driver="GeoJSON")