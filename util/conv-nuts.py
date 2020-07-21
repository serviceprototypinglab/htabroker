## Origin: geofencing.gitlab/nuts

import pandas as pd
import json

#url = "https://ec.europa.eu/eurostat/documents/345175/629341/NUTS2016-NUTS2021.xlsx"
url = "NUTS2016-NUTS2021.xlsx"

last = {}
regs = {}

debug = True

df = pd.read_excel(url, sheet_name="NUTS & SR 2021")
for idx, row in df.iterrows():
    cleanrow = {}
    for column in df.columns:
        if type(row.loc[column]) == str:
            b = row.loc[column].strip()
            if not b:
                b = None
            cleanrow[column] = b
        else:
            cleanrow[column] = row.loc[column]

    if type(cleanrow["Country"]) == str and cleanrow["Country"]:
        last["NUTS level 1"] = None
        last["NUTS level 2"] = None
        last["NUTS level 3"] = None
    if type(cleanrow["NUTS level 1"]) == str and cleanrow["NUTS level 1"]:
        last["NUTS level 2"] = None
        last["NUTS level 3"] = None
    if type(cleanrow["NUTS level 2"]) == str and cleanrow["NUTS level 2"]:
        last["NUTS level 3"] = None
    for column in df.columns:
        if type(cleanrow[column]) != str:
            if column in last:
                cleanrow[column] = last[column]
            else:
                cleanrow[column] = None

    if debug:
        identifier = "({}) {}".format(cleanrow["Code 2021"], cleanrow["Country"])
        if cleanrow["NUTS level 1"]:
            identifier += " |1> {}".format(cleanrow["NUTS level 1"])
        if cleanrow["NUTS level 2"]:
            identifier += " |2> {}".format(cleanrow["NUTS level 2"])
        if cleanrow["NUTS level 3"]:
            identifier += " |3> {}".format(cleanrow["NUTS level 3"])
        #regs.append(identifier)
        print(identifier)

    c = cleanrow["Country"]
    if not c in regs:
        regs[c] = {}
    if cleanrow["NUTS level 1"] and not cleanrow["NUTS level 2"]:
        regs[c][cleanrow["NUTS level 1"]] = {}
    if cleanrow["NUTS level 2"] and not cleanrow["NUTS level 3"]:
        regs[c][cleanrow["NUTS level 1"]][cleanrow["NUTS level 2"]] = {}
    if cleanrow["NUTS level 3"]:
        regs[c][cleanrow["NUTS level 1"]][cleanrow["NUTS level 2"]][cleanrow["NUTS level 3"]] = {}

    #for column in df.columns:
    #    print("//", column, row.loc[column])

    for column in df.columns:
        if type(cleanrow[column]) == str:
            last[column] = cleanrow[column]

f = open("nutsnames.json", "w")
json.dump(regs, f, ensure_ascii=False, indent=2)
f.close()
