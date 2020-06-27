## Overview
The benchmark seed data consists of metadata-files describing the relation between floors and sensors in the source system, and files containing data-entries for each timestamp in the database.

A seed generator is included which can generate this seed data at varying scales if you do not have your own seed data. Do note that the generator does not create realistic datasets and the resulting data should not be used for actual benchmarking because the cache- and compression-behaviors of the database is likely to differ significantly compared to using real data.

As of writing this, all of these seed metadata files must be present during seed data parsing. However, the `combined.csv`, `ignored.csv`, and `idmap.csv` can be left empty if they are unneeded.

## About the included sample data.
The sample data files included in this folder was generated using the seed-generator program with the following arguments: 
`java -jar path/to/seed-generator-1.0-SNAPSHOT.jar -f 4 -s 35 --separator=',' --time-start=2020-01-01 --time-end=2020-01-04 --gen-special=true --create-idmap=true`

The generated metadata-files were included as-is, while the generated data-entry files were truncated from 200.000+ lines to a representative sample of less than 500 lines. Because these entry-files were truncated you will need to regenerate the seed data before using it. 

## File overview
### floors.csv
This file provides an overview of the floors in the seed data-set and whether they are eligible for scaling when loaded into the benchmark.

When the benchmark up-scales the number of floors, it duplicates a floor that is eligible for scaling and reuses data for the access points on that floor (with slight modifications). The marking of 'ineligible' floors is mainly intended for buildings with special floors for which this upscaling would be misleading. For example, buildings with a ground-floor whose room-configuration differs massively from all other floors should mark the ground-floor as ineligible for scaling.

File format: Each line contains a floorkey (the name of the floor) and a boolean (true/false) indicating whether it is eligible for scaling. See the sample file for a concrete example of the format.

### floormap.csv
This file provides an overview of the access points present in the seed data by mapping them to a floor. Each access point can only be mapped to a single floor.

File format: Each line contains a floorkey (the name of the floor) and the name of an access point. See the sample file for a concrete example of the format.

### combined.csv
This file describes combinations of access points whose data in the data-entries should be combined during benchmark data-generation. This is intended for access points in the seed data that were renamed during the data-period but where the old data was never cleaned up.

File format: Each line contains a list of access point names that should be combined. Access points combined in this way must all be present in the floormap on the same floor. See the sample file for a concrete example of the format.

### ignored.csv
This file contains the names of access points that are present in the data-entries but should be ignored. Access points in this file can be present in `floormap.csv` but are not allowed to be present in `combined.csv`.

This may be useful if the seed data-entries contains access points whose location is unknown.

File format: Each line contains a single access point name to ignore. See the sample file for a concrete example of the format.

### idmap.csv
Sometimes, access point names are long descriptive pieces of text describing their name and location. This is great from a documentation point of view, but exporting this data to data-entries that contain these long names hundreds of thousands of times can cause unnecessarily large data-files.

This file is intended as a mapping from a short, compression-friendly name in exported data-entries to the readable name used in the other seed files. Ideally this file should be populated programmatically during seed data export where anytime a new access point name is encountered it is given a unique short name (e.g. an integer) instead and the mapping is written to this file.

File format: Each line contains a short name (as used in the data-entries) and then the name of the access point (as used in the other seed files). See the sample file for a concrete example of the format.

### Data-entries
Data-entries contains data for how many clients are connected to each access point at each point in time. To facilitate scaling of the seed data, the number of connected clients per access point is expressed as a percentage of the total so that we can preserve the system-wide total client number while scaling the number of access points if so desired (or we can scale the system-wide total clients without increasing the number of access points).

File format: Each file contains data-entries for a single date. Data-entries span several lines and their header is formatted as follows (using `,` as our separator):
```
Time,<timestamp: timestamp of this entry>
Total clients,<integer: total connected clients at this time>
```
The data for this entry then follows on their own lines. This data is formatted as follows:
```
<string: access point name>,<double: percent of total clients connected to this access point from 0.0 to 1.0>
```
If no data is reported for this timestamp, `NO DATA` can alternately be included here instead. This is an artifact from the initial seed data extraction script I wrote and such entries are ignored entirely during parsing as if they weren't present.

An example of a valid data-entry could be as follows, where 12 clients are connected to the occupancy system, spread out over the 4 access points that the system has:
```
Time,2020-06-15T15:23:00
Total clients,12
AP-LARGE-ROOM,0.5
AP-SMALL-ROOM,0.25
AP-MEETING-ROOM,0.25
AP-EMPTY-ROOM,0.0
```
