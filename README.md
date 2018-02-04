
# Learn (Master) Apache Spark

Useful resources (without particular order)

## Topics

### Core concepts
- Driver program, Worker Node (Executore, Tasks), Spark cluster
- Actions vs Transformations
- SparkContext
- reduce(), fold()
- Persistence levels: MEMORY_ONLY, MEMORY_ONLY_SER, MEMORY_AND_DISK, MEMORY_AND_DISK_SER, DISK_ONLY

### RDD
- Partitions
- persist (memory, disk)
- Creating RDD (parallelize(), load from external storage: textFile())
- Lineage graph
- ETL (Extract, Transform, Load)
- Pair RDD: reduceByKey(), groupByKey(), combineByKey(), mapValues()
- repartition(), coalesce()
- joins
- HashPartitioner

### DataFrame

## Books
- Learning Spark by Matei Zaharia, Patrick Wendell, Andy Konwinski, Holden Karau
- Spark in Action

## Misc
- http://spark.apache.org/docs/latest/

## Courses:
- [course_safari_lp_abbda](https://www.safaribooksonline.com/learning-paths/learning-path-architect/9781491987063) - Chapter: Apache Spark
