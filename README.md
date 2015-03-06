# pig-hll
Pig Hyperloglog UDFs

This project implements a PIG wrapper around Aggregate Knowledge's awesome Java HLL implementation. (https://github.com/aggregateknowledge/java-hll)
The HLL outputs can be imported into Postgres for further analysis. (https://github.com/aggregateknowledge/postgresql-hll)

For analysis of HLLs in MySql, (https://github.com/amirtuval/pig-hyperloglog) can be used.

Usage
Register the xad-pig_hll-1.0.jar in your PIG.

The following UDFs are provided:

HLL_CREATE - Given a list of values, this function will return the HLL string computed from these values.
HLL_COMPUTE - Given a list of values, this function will return an integer representing the estimated distinct count of these values.
HLL_MERGE - Given a list of HLL strings, this function will return the HLL string that is the combination of all the hll strings.
HLL_MERGE_COMPUTE - Given a list of HLL strings, this function will return an integer representing the estimated distinct count of these values.

Each HLL string is store in Hex form. Further details for tuning/optimization can be found in Aggregate Knowledge's page.
https://github.com/aggregateknowledge/java-hll

Compilation:

mvn clean package






