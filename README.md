# 🏭 PFB Exporter

Transform and export data from a relational database into a
PFB (Portable Format for Bioinformatics) file.

A PFB file is an Avro file containing a schema and data that originally came
from a relational database but has transformed into a graph structure suitable
for capturing and reconstructing relational data.

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
A serialized file (binary or JSON) (also can be compressed) file with a
schema and data in it. Read more about
[Avro](https://Avro.apache.org/docs/current/spec.html).

## Avro Basics

### Avro File
The writer takes in a schema (JSON file) and the data which conforms to that
schema, and writes it to an Avro file. The schema gets written first,
then the data records.

### Write Avro
The Avro schema is pretty simple. Its a JSON file. It has entities,
their attributes, and the types of those attributes.
You can represent primitive types and complex types in order to represent
the schema for complicated nested JSON structures.

### Read Avro
The reader doesn't need the schema since its embedded in the data.
The reader reads in and parses the Avro file to JSON.

## Vanilla Avro vs PFB
Let's say a client receives an Avro file. It reads in the Avro data.
Now a client has the Avro schema and all of the data that conforms to that
schema in a big JSON blob. It can do what it wants. Maybe it wants to construct
some data input forms. To do this it has everything it needs since the schema
has all of the entities, attributes, and types for those attributes defined.

Now what happens if the client wants to reconstruct a relational database
from the data? How does it know what tables to create, and what the
relationships are between those tables? Which relationships are
required vs not?

This is where PFB comes in. PFB is also an Avro file except that the data
inside a PFB Avro file has been transformed into a graph
(adjacency list) making it suitable to capture and reconstruct data
from relational databases.
