# SharedStreets Matcher

This is a cluster processing system for converting GPS tracess into speed and location observations. Built using Apache Flink and auto scales to use all available processors, and can be deployed on a Flink cluster for distributed processing. 

As inputs this takes a directory of SharedStreets map tiles, and text files files containing GPS event streams. Outputs speed histograms for SharedStreets segments and SharedStreets referenced locations for driver events.

**Build** 

`gradle build` 

**Run**

`java -jar sharedstreets-matcher.jar --map [path/to/map/tiles...] --input [path/to//input/data...] --output [output/path]`


