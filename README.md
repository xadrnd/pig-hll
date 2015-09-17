# pig-hll
Pig Hyperloglog UDFs

This project implements a PIG wrapper around Aggregate Knowledge's awesome Java HLL implementation. (https://github.com/aggregateknowledge/java-hll)

The UDF's can be used to produce *HLL strings* which can then be imported into Postgres for further analysis

See:
* https://github.com/aggregateknowledge/postgresql-hll
* https://github.com/aggregateknowledge/hll-storage-spec



Usage:

Register the xad-pig_hll-1.0.jar in your PIG script.

The following UDFs are provided:

UDF   |Description
------|---------------------------------------
HLL_CREATE|Given a *list of values*, create/return the *HLL string* representation of the set
HLL_COMPUTE|Given a *list of values*, compute/return the *estimated cardinality* of the set
HLL_MERGE|Merge a list of *HLL strings* into a single merged *HLL string* representation of the set
HLL_MERGE_COMPUTE|Merge a list of *HLL strings* and compute/return the *estimated cardinality* of the set


Compilation:
```
mvn clean package
```





