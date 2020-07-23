# Hierarchical Topic Aggregation broker prototype

import json
import string
import sys
import glob
import os
import hashlib

sys.path.append("lib")

hierarchy_data = {}
hierarchy_geohash = {}

sublist = []

pulsarurl = "pulsar://localhost:6650"

print("Hierarchical topic aggregation broker prototype.")

print("+ backend: simulation active")

client = None
consumers = {}
try:
    import pulsar
    client = pulsar.Client(pulsarurl)
    testproducer = client.create_producer("non-persistent://public/default/htabroker-testprod")
    print(f"+ backend: pulsar (client to {pulsarurl}) active")
except:
    if client:
        client.close()
        client = None
    print("- backend: pulsar inactive")

def ndcount(d):
    if isinstance(d, dict) and len(d) == 0:
        return 1
    return sum([ndcount(v) + 1 if isinstance(v, dict) else 1 for v in d.values()])

def flat(l):
    return [y for x in l for y in x]

def qualify(k, lv):
    return [k + "::" + v for v in lv]

def ndkeys(d):
    return list(d.keys()) + flat([qualify(k, ndkeys(v)) for k, v in d.items() if isinstance(v, dict)])

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

def hashedtopic(t):
    return hashlib.sha1(t.encode("utf-8")).hexdigest()

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
        if args == k:
            return h, args
        x = smartsearch(args, h[k])
        if x:
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
    pub = args[0]
    print("# sim: published to", pub, msg)

    if client:
        producer = client.create_producer(f"non-persistent://public/default/{hashedtopic(pub)}")
        if producer:
            producer.send(msg.encode("utf-8"))
            print("# pulsar: published (generic)", msg)

    props = []
    for s in sublist:
        h, sresargs = resolver(s)
        if sresargs in resargs:
            print("PROPAGATE UP:", s, "because", resargs, "∈", sresargs)
            props.append(s)
        elif resargs in sresargs:
            print("PROPAGATE DOWN:", s, "because", sresargs, "∈", resargs)
            props.append(s)
        else:
            print("# skip propagation", s, "because", resargs, "∉", sresargs)

    for prop in props:
        if client:
            producer = client.create_producer(f"non-persistent://public/default/{hashedtopic(prop)}")
            if producer:
                producer.send(msg.encode("utf-8"))
    if props and client:
        print("# pulsar: propagated, total", len(props))

def subscribe(args):
    if len(args) > 1:
        print("E: Too many arguments.")
        return
    h = resolver(args[0])
    if not h:
        return
    sub = args[0]
    sublist.append(sub)
    if client:
        consumers[sub] = client.subscribe(f"non-persistent://public/default/{hashedtopic(sub)}", subscription_name="echo")
    print("# subscribed to", sub)
    print("# list", sublist)

def check(args):
    if len(args) > 0:
        print("E: Too many arguments.")
        return
    if not consumers:
        print("E: No consumer established.")
        return
    if not client:
        print("E: Pulsar backend not available.")
        return
    for sub in sublist:
        consumer = consumers[sub]
        try:
            msg = consumer.receive(timeout_millis=50)
        except:
            print("# pulsar: nothing received on", sub)
            return
        m = msg.data().decode()
        #consumer.acknowledge(msg)
        print("# pulsar: received", m, "on", sub)

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
