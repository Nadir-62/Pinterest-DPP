# Pinterest-DPP
Pinterest Data Processing Pipeline


## Milestone 2 - Briefing
- Pinterest Daily deadlines were not met as their previous legacy system could not process data fast enough

Their new pipeline has 3 main requirments:
- It had to be extencible enough so that new metrics could be added and back filled historically
- It had to be able to process a massive amount of data by a rapidly growing user base
- It has to be able to create dashboards using recent and historical data

Two processes are therefore run parallel to each other. The "Batch" and "Real Time" processing units.

Spark is 100 times faster than Hadoop as it executes commands in memory instead of reading and writing from a hard disk.
