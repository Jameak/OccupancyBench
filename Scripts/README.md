# Script overview
### comparequeries.py
For use during development of new database query-implementations to check that the results returned from the database implementations match when executed with the same query-arguments. Due to schema- and datastructure-differences there are slight differences in the query-output of some implementations. This script tries to consider these when comparing the output, but this wasn't implemented for cross-schema K-Means query-result comparisons due to time-considerations during my thesis.

To create the files used by this script, run the benchmark with the `debug.savequeryresults` option enabled.

Note that to get comparable query-results, make sure that the configuration of both benchmark-runs is identical (except for the targeted database) and that ingestion is disabled. If comparisons across schema-implementations is desired, the following additional debug options should be used:
* Enable `debug.synchronizerngstate` to ensure that timestamp generation retrieves an equivalent number of values from the RNG-source for both schemas to keep the RNG synced.
* Enable `debug.truncatequerytimestamps` to tell the query-argument generator to truncate timestamps such that the minor timestamp differences between the row- and column-schemas dont impact query-arguments.

### extractSeedData.py
Extracts seed data from an InfluxDB database into the format expected by the benchmark. This was specifically written to extract the data from the ITU occupancy database and may not be useful for anyone else looking to extract seed data from their own systems. However, it is included as a potential starting point for such implementations.

