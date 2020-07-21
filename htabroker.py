import json
import string
import sys
import glob
import os

sys.path.append("lib")

hierarchy_data = {}
hierarchy_geohash = {}

sublist = []

pulsarurl = "pulsar://localhost:6650"

print("Hierarchical topic aggregation broker prototype.")

print("+ backend: simulation active")

client = None
producer = None
consumer = None
try:
    import pulsar
    client = pulsar.Client(pulsarurl)
    producer = client.create_producer("non-persistent://public/default/minibroker")
    consumer = client.subscribe("non-persistent://public/default/minibroker", subscription_name="echo")
    print(f"+ backend: pulsar (client to {pulsarurl}) active")
except:
    if client:
        client.close()
        client = None
    print("- backend: pulsar inactive")

def ndcount(d):
    if isinstance(d, dict) and len(d) == 0:
        return 1
    return sum([ndcount(v) if isinstance(v, dict) else 1 for v in d.values()])

data = glob.glob("data/*.json")
data = [os.path.basename(d).replace(".json", "") for d in data]

for d in data:
    try:
        hierarchy_data[d] = json.load(open(f"data/{d}.json"))
        print(f"+ model: {d} active")
        print("  > entries", ndcount(hierarchy_data[d]))
    except:
        print(f"- model: {d} inactive")

def buildgeo(level, prefix=""):
    if level == 0:
        return {}
    h = {}
    for c in string.digits + string.ascii_lowercase:
        h[prefix + c] = buildgeo(level - 1, prefix + c)
    return h

try:
    import geohash
    print("+ model: geohash (built-in) active")
    hierarchy_geohash = buildgeo(3)
    print(f"  > entries", ndcount(hierarchy_geohash))
except:
    print("- model: geohash (built-in) inactive")

hierarchy = {}
for d in hierarchy_data:
    hierarchy.update(hierarchy_data[d].copy())
hierarchy.update(hierarchy_geohash.copy())

def helpmenu():
    print("---")
    print("Subscribe: sub <topic>")
    print("Publish: pub <topic> <msg>")
    print("List: list [<topic>[::<subtopic>]...]")
    print("Check: check")
    print("Help: help / ?")
    print("---")

def smartsearch(args, h):
    for k in h:
        #print("&", args, k)
        if args == k:
            return h, args
        x = smartsearch(args, h[k])
        if x:
            #print("X", x)
            h, resargs = x
            return h, k + "::" + resargs
    return None

def resolver(arg):
    resargs = ""
    args = arg.split("::")
    h = hierarchy
    for arg in args:
        print("# filter", arg)
        found = False
        for k in h:
            kparts = k.split("/")
            for kpart in kparts:
                if kpart == arg:
                    h = h[k]
                    found = True
                    if resargs:
                        resargs += "::"
                    resargs += k
                    break
            if found:
                break
        if not found:
            print("W: Topic", arg, "not found; trying smart search.")
            h = smartsearch(arg, hierarchy)
            if not h:
                print("E: Topic", arg, "definitely not found.")
                return
            h, resargs = h
            h = h[arg]
            print("# resolving to", resargs)
    return h, resargs

def listtopics(args):
    if len(args) > 1:
        args = [" ".join(args)]
        #print("E: Too many arguments.")
        #return
    if len(args) == 1 and not args[0].strip():
        args = []
    if len(args) == 1:
        h = resolver(args[0])
        if not h:
            return
        h, resargs = h
    else:
        h = hierarchy
    if not len(h):
        print("(No further entries.)")
    for k in h:
        print(k)

def publish(args):
    if len(args) < 2:
        print("E: Not enough arguments.")
        return
    msg = " ".join(args[1:])
    if not msg:
        print("E: Empty message.")
        return
    h = resolver(args[0])
    if not h:
        return
    h, resargs = h
    print("# sim: published to", args[0], msg)

    if producer:
        producer.send(msg.encode("utf-8"))
        print("# pulsar: published (generic)", msg)

    for s in sublist:
        h, sresargs = resolver(s)
        if sresargs in resargs:
            print("PROPAGATE UP:", s, "because", resargs, "∈", sresargs)
        elif resargs in sresargs:
            print("PROPAGATE DOWN:", s, "because", sresargs, "∈", resargs)
        else:
            print("# skip propagation", s, "because", resargs, "∉", sresargs)

def subscribe(args):
    if len(args) > 1:
        print("E: Too many arguments.")
        return
    h = resolver(args[0])
    if not h:
        return
    sublist.append(args[0])
    print("# subscribed to", args[0])
    print("# list", sublist)

def check(args):
    if len(args) > 0:
        print("E: Too many arguments.")
        return
    if not consumer:
        print("E: No consumer established.")
        return
    msg = consumer.receive()
    m = msg.data().decode()
    #consumer.acknowledge(msg)
    print("# pulsar: received", m)

def main():
    while True:
        cmd = input("))) ")

        tokens = cmd.split(" ")
        if len(tokens) == 0:
            continue
        if tokens[0] == "help" or tokens[0] == "?":
            helpmenu()
        elif tokens[0] == "list":
            listtopics(tokens[1:])
        elif tokens[0] == "check":
            check(tokens[1:])
        elif tokens[0] == "pub":
            publish(tokens[1:])
        elif tokens[0] == "sub":
            subscribe(tokens[1:])

helpmenu()
try:
    main()
except KeyboardInterrupt:
    print("Begone.")
except EOFError:
    print("Begone.")
except Exception as e:
    print("Begone with error.", e, type(e))

if client:
    client.close()
