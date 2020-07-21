import pandas as pd
import json

url = "be-b-00.04-agv-01.xlsx"

regs = {}

df = pd.read_excel(url, sheet_name="GDE")
for idx, row in df.iterrows():
    g = row.loc["GDENAME"]
    b = row.loc["GDEBZNA"]
    k = row.loc["GDEKTNA"]

    if not k in regs:
        regs[k] = {}
    if not b in regs[k]:
        regs[k][b] = {}
    regs[k][b][g] = {}

f = open("communes.json", "w")
json.dump(regs, f, ensure_ascii=False, indent=2)
f.close()
