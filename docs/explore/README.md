# Exploration of PFB
Explore PFB (portable bioinformatics format)

https://github.com/uc-cdis/pypfb

## Setup for Jupyter Notebook

```shell
$ git clone git@github.com:d3b-center/d3b-lib-pfb-exporter.git
$ cd docs/explore
$ python3 -m venv venv
$ source venv/bin/activate
$ pip install -r requirements.txt
$ jupyter
```

# Background

## What is an Avro File?
A binary compressed file with a schema and data in it.

## Avro Basics

### Avro File
The writer takes in a schema (JSON file) and the data which conforms to that schema, and writes it to an avro file. The schema gets written first, then the data.

### Write Avro
The avro schema is pretty simple. Its a JSON file. It has entities, their attributes, and the types of those attributes. You can represent primitive types and complex types in order to represent the schema for complicated nested JSON structures. Read more about [avro](https://avro.apache.org/docs/current/spec.html).

### Read Avro
The reader doesn't need the schema since its embedded in the data. The reader reads in and parses the avro file to JSON.

## Vanilla Avro vs PFB
Let's say a client receives an avro file. It reads in the avro data. Now a client has the avro schema and all of the data that conforms to that schema in a big JSON blob. It can do what it wants. Maybe it wants to construct some data input forms. To do this it has everything it needs since the schema has all of the entities, attributes, and types for those attributes defined.

Now what happens if the client wants to reconstruct a relational database from the data? How does it know what tables to create, and what the relationships are between those tables? Which relationships are required vs not?

This is where PFB comes in. PFB is also an avro file except that the data
inside a PFB avro file has been transformed into a somewhat generic graph
(adjacency list) making it suitable to capture and reconstruct data
from relational databases.

## PFB CLI (pypfb Python package)

1. Convert a Gen3 data dictionary into an empty graph encoded in JSON
2. Transform the input data to fit into the graph
3. Create an avro schema for the graph and add it to the avro file
4. Add the graph with the data to the avro file

PFB CLI's main goal is to capture data from relational databases and serialize
it in an efficient form so that it can be transported and used to reconstruct
the original relational database.


![How pypfb Works](../source/_static/images/pfb.png)

### Issues with the pypfb
- PFB is really just a graph structure that can be used to capture graph like
data. The graph structure is known as the PFB Schema
- PFB does NOT try to preserve the original structure of the data. Rather, it
preserves the entity definitions and relationships. This means it should not
be used to migrate data between two instances of the same system. For example,
we should not use it to migrate data from one Kids First Data Service
deployment to another.
- The conversion from a Gen3 data dictionary to an Avro schema to represent
the graph structure seems overly complex and completely unnecessary for data
that does not come from Gen3.
- The PFB concept - transform data into a generic graph structure for the
purpose of capturing and reconstructing relational data - is a good one

### Issues with the CLI
- The CLI is very slow (pfb -h takes 3 seconds, why?)
- Needs to be cleaned up and refactored - code is difficult to follow and
there are large commented sections

## Apache Avro
- Don't use this
- Apache's python avro package is super slow because its written in pure Python
- Also the setup.py is missing pycodestyle dependency pypy avro package
can't find StringIO module. I think you need the snappy codec package for it to work.

## Fast Avro
- pypfb uses this package
- Written in cpython so its way faster than Apache's Python package
- Doesn't support schema hashing or parsing into canonical form
(needed for diffing two schemas)

## Recommendations
- Create a PFB CLI which can point to a relational database and
generate a PFB file
- The tool should inspect the database and directly produce the PFB schema
(skip the intermediate Gen3 data dictionary layer)
- The tool should transform data from the database into PFB entities
- The tool should have an option to download the data from the database.
Otherwise, a directory of JSON payloads representing rows from tables the
database should be provided to the tool for PFB entity creation
