# 🏭 PFB Exporter

Transform and export data from a relational database into a
PFB (Portable Format for Bioinformatics) file.

A PFB file is special kind of Avro file, suitable for capturing and reconstructing relational data. Read Background for more information.

## Quickstart

```shell
$ git clone git@github.com:d3b-center/d3b-lib-pfb-exporter.git
$ cd d3b-lib-pfb-exporter
$ python3 -m venv venv
$ source venv/bin/activate
$ pip install -e .
$ pfbe -h
```

Try it out:

**🚧 Does not generate a PFB yet 🚧**

```shell
$ pfbe export tests/data/input -m tests/data/input -o tests/data/pfb_export
```

## Developers

Follow Quickstart instructions first. Then install dev requirements:

```shell
$ pip install -r dev-requirements.txt
```

# Background

## What is an Avro File?
A file with data records (JSON) and a schema (JSON) to describe each data
record. Avro files can be serialized into a binary format and compressed.

Read more about [Avro](https://Avro.apache.org/docs/current/spec.html).

## What is a PFB File?

A PFB file is special kind of Avro file, suitable for capturing and
reconstructing biomedical relational data.

A PFB file is an Avro file with a particular schema that represents a graph
structure (adjacency list). We call this schema the
[PFB Schema](https://github.com/uc-cdis/pypfb/tree/master/doc)

The graph structure consists of a list of JSON objects called PFB Entity
objects. Each PFB Entity conforms to the PFB Schema. A PFB Entity has
attributes + values, relations to other PFB Entities, and ontology references.
The ontology references can be attached to the PFB Entity and also to each
attribute of the PFB Entity.

The data records in a PFB file are produced by transforming the original data
from a relational database into PFB Entity objects.

![How PFB Exporter Works](docs/source/_static/images/pfb-exporter.png)

## Vanilla Avro vs PFB
Let's say a client receives an Avro file. It reads in the Avro data.
Now a client has the Avro schema and all of the data that conforms to that
schema in a big JSON blob. It can do what it wants. Maybe it wants to construct
some data input forms. It has everything it needs to do this since the schema
has all of the entities, attributes, and types for those attributes defined.

Now what happens if the client wants to reconstruct a relational database
from the data? How does it know what tables to create, and what the
relationships are between those tables? Which relationships are
required vs not? This is one of the problems PFB addresses.
