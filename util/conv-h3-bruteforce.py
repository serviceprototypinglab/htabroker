# Calculates the bounding box of a shape and does a full gridding to determine which points are in it.
# For each point, produces the H3 hexagonal hierarchical geospatial index.
# Might require: sudo apt-get install python3-pyshp

#import geopandas as gpd
import shapefile
from shapely.geometry import Point, shape
import numpy as np
import subprocess
import json
import sys

if len(sys.argv) < 2 or len(sys.argv) > 4:
    print("Syntax: bruteforce.ch <file.shp> [<scansize(°)>:0.01] [<resolution>:6]", file=sys.stderr)
    exit(1)

shapefn = sys.argv[1]
#shapefn = "CHE_adm1.shp"
scansize = 0.01
if len(sys.argv) >= 3:
    scansize = float(sys.argv[2])
resolution = 6
if len(sys.argv) >= 4:
    resolution = int(sys.argv[3])

def getbounds(shp):
    bs = [None, None, None, None]
    all_shapes = shp.shapes()
    for i in range(len(all_shapes)):
        boundary = all_shapes[i]
        b = shape(boundary).bounds
        if bs[0] is None or b[0] < bs[0]:
            bs[0] = b[0]
        if bs[1] is None or b[1] < bs[1]:
            bs[1] = b[1]
        if bs[2] is None or b[2] > bs[2]:
            bs[2] = b[2]
        if bs[3] is None or b[3] > bs[3]:
            bs[3] = b[3]
    #print("BS", bs)
    return bs

def checkpoint(shp, lon, lat, r):
    all_shapes = shp.shapes()
    all_records = shp.records()
    for i in range(len(all_shapes)):
        boundary = all_shapes[i]
        if Point((lon, lat)).within(shape(boundary)):
           name = all_records[i][2]
           print(lon, lat, name)
           p = subprocess.run(f"./h3.github/bin/geoToH3 -r {r} --lon {lon} --lat {lat}", shell=True, stdout=subprocess.PIPE)
           idx = p.stdout.decode().split("\n")[0]
           return idx, name
    #print("Nothing found", lon, lat)

def producefile(shapefn, outfile, scansize, resolution):
    #df = gpd.read_file(shapefn)
    shp = shapefile.Reader(shapefn)
    bs = getbounds(shp)

    idxlist = {}
    for lon in np.arange(bs[0], bs[2], scansize):
        for lat in np.arange(bs[1], bs[3], scansize):
            res = checkpoint(shp, lon, lat, resolution)
            if not res:
                continue
            idx, name = res
            if not name in idxlist:
                idxlist[name] = []
            idxlist[name].append(idx)

    idxcomp = {}
    for name in idxlist:
        idxcomp[name] = list(set(idxlist[name]))
        print("Total indices for", name, ":", len(idxlist[name]), "unique", len(idxcomp[name]), ".")

        if len(idxlist[name]) > len(idxcomp[name]) * 4:
            print("ADVICE: Too many duplicates. Increase scansize or increase resolution.")
        elif len(idxlist[name]) == len(idxcomp[name]):
            print("ADVICE: No duplicates. Risk of missing hexagons. Decrease scansize or decrease resolution.")

    elist = {}
    for name in idxcomp:
        elist[name] = {k: {} for k in idxcomp[name]}
    #d = {f"H3R{resolution}-{shapefn}": elist}
    d = elist

    f = open(outfile, "w")
    json.dump(d, f, ensure_ascii=False, indent=2)
    f.close()

producefile(shapefn, "h3.json", scansize, resolution)
