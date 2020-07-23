# Hierarchical Topic Aggregation broker prototype

import json
import string
import sys
import glob
import os
import hashlib

## Helper functions

def ndcount(d):
    return sum([ndcount(v) + 1 if isinstance(v, dict) else 1 for v in d.values()])

def flat(l):
    return [y for x in l for y in x]

def qualify(k, lv):
    return [k + "::" + v for v in lv]

def ndkeys(d):
    return list(d.keys()) + flat([qualify(k, ndkeys(v)) for k, v in d.items() if isinstance(v, dict)])

def buildgeo(level, prefix=""):
    if level == 0:
        return {}
    h = {}
    for c in string.digits + string.ascii_lowercase:
        h[prefix + c] = buildgeo(level - 1, prefix + c)
    return h

def hashedtopic(t):
    return hashlib.sha1(t.encode("utf-8")).hexdigest()

## Options container, for modification through argparse

class Options:
    def __init__(self):
        self.pulsarurl = "pulsar://localhost:6650"

## Main class

class HTABroker:
    def __init__(self, options):
        self.options = options
        self.client = None
        self.consumers = {}
        self.hierarchy = {}
        self.sublist = []

    def startcli(self):
        print("Hierarchical topic aggregation broker prototype.")
        self.load_backends()
        self.load_models()
        self.helpmenu()
        self.cliloop()

    def load_backends(self):
        print("+ backend: simulation active")

        self.client = None
        try:
            import pulsar
            self.client = pulsar.Client(pulsarurl)
            testproducer = self.client.create_producer("non-persistent://public/default/htabroker-testprod")
            print(f"+ backend: pulsar (client to {pulsarurl}) active")
        except:
            if self.client:
                self.client.close()
            print("- backend: pulsar inactive")

    def load_models(self):
        sys.path.append("lib")

        hierarchy_data = {}
        hierarchy_geohash = {}

        data = glob.glob("data/*.json")
        data = [os.path.basename(d).replace(".json", "") for d in data]

        for d in data:
            try:
                hierarchy_data[d] = json.load(open(f"data/{d}.json"))
                print(f"+ model: {d} active")
                print("  > entries", ndcount(hierarchy_data[d]))
            except:
                print(f"- model: {d} inactive")

        try:
            import geohash
            print("+ model: geohash (built-in) active")
            hierarchy_geohash = buildgeo(3)
            print(f"  > entries", ndcount(hierarchy_geohash))
        except:
            print("- model: geohash (built-in) inactive")

        self.hierarchy = {}
        for d in hierarchy_data:
            self.hierarchy.update(hierarchy_data[d].copy())
        self.hierarchy.update(hierarchy_geohash.copy())

    def helpmenu(self):
        print("---")
        print("Subscribe: sub <topic>")
        print("Publish: pub <topic> <msg>")
        print("List: list [<topic>[::<subtopic>]...]")
        print("Check: check")
        print("Help: help / ?")
        print("---")

    def smartsearch(self, args, h):
        for k in h:
            if args == k:
                return h, args
            x = self.smartsearch(args, h[k])
            if x:
                h, resargs = x
                return h, k + "::" + resargs
        return None

    def resolver(self, arg):
        resargs = ""
        args = arg.split("::")
        h = self.hierarchy
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
                h = self.smartsearch(arg, self.hierarchy)
                if not h:
                    print("E: Topic", arg, "definitely not found.")
                    return
                h, resargs = h
                h = h[arg]
                print("# resolving to", resargs)
        return h, resargs

    def listtopics(self, args):
        if len(args) > 1:
            args = [" ".join(args)]
        if len(args) == 1 and not args[0].strip():
            args = []
        if len(args) == 1:
            h = self.resolver(args[0])
            if not h:
                return
            h, resargs = h
        else:
            h = self.hierarchy
        if not len(h):
            print("(No further entries.)")
        for k in h:
            print(k)

    def publish(self, args):
        if len(args) < 2:
            print("E: Not enough arguments.")
            return
        msg = " ".join(args[1:])
        if not msg:
            print("E: Empty message.")
            return
        h = self.resolver(args[0])
        if not h:
            return
        h, resargs = h
        pub = args[0]
        print("# sim: published to", pub, msg)

        if self.client:
            producer = self.client.create_producer(f"non-persistent://public/default/{hashedtopic(pub)}")
            if producer:
                producer.send(msg.encode("utf-8"))
                print("# pulsar: published (generic)", msg)

        props = []
        for s in self.sublist:
            h, sresargs = self.resolver(s)
            if sresargs in resargs:
                print("PROPAGATE UP:", s, "because", resargs, "∈", sresargs)
                props.append(s)
            elif resargs in sresargs:
                print("PROPAGATE DOWN:", s, "because", sresargs, "∈", resargs)
                props.append(s)
            else:
                print("# skip propagation", s, "because", resargs, "∉", sresargs)

        for prop in props:
            if self.client:
                producer = self.client.create_producer(f"non-persistent://public/default/{hashedtopic(prop)}")
                if producer:
                    producer.send(msg.encode("utf-8"))
        if props and self.client:
            print("# pulsar: propagated, total", len(props))

    def subscribe(self, args):
        if len(args) > 1:
            print("E: Too many arguments.")
            return
        h = self.resolver(args[0])
        if not h:
            return
        h, resargs = h
        sub = args[0]
        self.sublist.append(sub)
        if self.client:
            self.consumers[sub] = self.client.subscribe(f"non-persistent://public/default/{hashedtopic(sub)}", subscription_name="echo")
        print("# subscribed to", sub)
        print("# list", self.sublist)

        for entry in ndkeys(self.hierarchy):
            if entry.startswith(resargs + "::"):
                print("# implies up-propagation subscription to", entry)
            if resargs.startswith(entry + "::"):
                print("# implies down-propagation subscription to", entry)

    def check(self, args):
        if len(args) > 0:
            print("E: Too many arguments.")
            return
        if not self.consumers:
            print("E: No consumer established.")
            return
        if not self.client:
            print("E: Pulsar backend not available.")
            return
        for sub in self.sublist:
            consumer = self.consumers[sub]
            try:
                msg = consumer.receive(timeout_millis=50)
            except:
                print("# pulsar: nothing received on", sub)
                return
            m = msg.data().decode()
            #consumer.acknowledge(msg)
            print("# pulsar: received", m, "on", sub)

    def main(self):
        while True:
            cmd = input("))) ")

            tokens = cmd.split(" ")
            if len(tokens) == 0:
                continue
            if tokens[0] == "help" or tokens[0] == "?":
                self.helpmenu()
            elif tokens[0] == "list":
                self.listtopics(tokens[1:])
            elif tokens[0] == "check":
                self.check(tokens[1:])
            elif tokens[0] == "pub":
                self.publish(tokens[1:])
            elif tokens[0] == "sub":
                self.subscribe(tokens[1:])

    def cliloop(self):
        try:
            self.main()
        except KeyboardInterrupt:
            print("Begone.")
        except EOFError:
            print("Begone.")
        except Exception as e:
            print("Begone with error.", e, type(e))

        if self.client:
            self.client.close()

opt = Options()

broker = HTABroker(opt)
broker.startcli()
