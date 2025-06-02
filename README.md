# Spark SQL & DataFrames Exercise Submission

**Name:** Mina Gharenazifam

**Setup:** Docker-based Spark using `spark-shell`  

**Input Format:** JSON  

**Execution Mode:** Interactive via terminal (`spark-shell` inside container)

**Docker File:** https://github.com/mrn-aglic/pyspark-playground

---

##  Your First Spark Wordcount
```bash
//bring up Spark cluster
docker compose up -d
```

**Add WordCount.scala and file.txt from local to container**
```bash
docker cp WordCount.scala da-spark-master:/opt/spark/apps/WordCount.scala
```
```
Successfully copied 2.56kB to da-spark-master:/opt/spark/apps/WordCount.scala
```
```bash
docker cp file.txt da-spark-master:/opt/spark/data/book_data/file.txt
```
```
Successfully copied 2.05kB to da-spark-master:/opt/spark/data/book_data/file.txt
```

**for execution, we first enter the container shell**
```bash
docker exec -it da-spark-master /bin/bash
```
```bash
spark-shell -i /opt/spark/apps/wordcount.scala
```
**Output**
```
Setting default log level to "WARN".
To adjust logging level use sc.setLogLevel(newLevel). For SparkR, use setLogLevel(newLevel).
25/06/01 20:27:14 WARN NativeCodeLoader: Unable to load native-hadoop library for your platform... using builtin-java classes where applicable
Spark context Web UI available at http://3b6570a0ae80:4040
Spark context available as 'sc' (master = spark://spark-master:7077, app id = app-20250601202715-0001).
Spark session available as 'spark'.
Welcome to                                                                      
      ____              __
     / __/__  ___ _____/ /__
    _\ \/ _ \/ _ `/ __/  '_/
   /___/ .__/\_,_/_/ /_/\_\   version 3.3.3
      /_/
         
Using Scala version 2.12.15 (OpenJDK 64-Bit Server VM, Java 11.0.27)
Type in expressions to have them evaluated.
Type :help for more information.
```
**Checking the results**
```bash
ls /opt/spark/data/book_data/output
```
```
_SUCCESS  part-00000  part-00001
```
```bash
cat /opt/spark/data/book_data/output/part-00000
```
```
(us,1)
(hello,1)
(world,1)
```

## Average of exam results per student

**first we need to move ```ExamResults.txt``` and ```average_grade.scala``` to container**

```bash
(base) minafam@Minas-MacBook-Pro book_data % docker cp average_grade.scala da-spark-master:/opt/spark/app
s/
Successfully copied 2.56kB to da-spark-master:/opt/spark/apps/
(base) minafam@Minas-MacBook-Pro book_data % docker cp examsResults.txt da-spark-master:/opt/spark/data/b
ook_data/examResults
```
**Run and Execution**
```bash
(base) minafam@Minas-MacBook-Pro pyspark-playground % docker exec -it da-spark-master spark-shell -i /opt/spark/apps/average_grade.scala
```
**Output**
```
Setting default log level to "WARN".
To adjust logging level use sc.setLogLevel(newLevel). For SparkR, use setLogLevel(newLevel).
25/06/01 21:47:28 WARN NativeCodeLoader: Unable to load native-hadoop library for your platform... using builtin-java classes where applicable
Spark context Web UI available at http://a692bfbbf9c3:4040
Spark context available as 'sc' (master = spark://spark-master:7077, app id = app-20250601214730-0000).
Spark session available as 'spark'.
+-------+-----------------+------------------+                                  
|  matno|             name|     average_grade|
+-------+-----------------+------------------+
|9000034|      Lukas Maier|               2.0|
|9000082|       Sofie Haas|               3.0|
|9000067|         Lea Böhm|2.6666666666666665|
|9000020|Sophie Zimmermann|2.6666666666666665|
|9000053|      Noah Keller|3.6666666666666665|
|9000038|      Paul Walter|3.3333333333333335|
|9000072|    Emilie Krämer|2.3333333333333335|
|9000024|    Lina Hartmann|3.3333333333333335|
|9000052| Niklas Friedrich|2.3333333333333335|
|9000029|      Maya Krause|1.6666666666666667|
|9000037|       Luka König|3.3333333333333335|
|9000086|    Nele Dietrich|               3.0|
|9000044|        Timm Lang|               3.0|
|9000023|    Sofia Hofmann|               3.0|
|9000018|     Lara Neumann|               2.0|
|9000033|    Lucas Schulze|               3.0|
|9000098|      Luka Arnold|               2.5|
|9000079|      Lara Seidel|               2.0|
|9000009|      Emma Schulz|               2.0|
|9000056|   Philipp Berger|3.6666666666666665|
+-------+-----------------+------------------+
only showing top 20 rows
```

## Add input Data: `people.json`

```json
{"name":"Michael"}
{"name":"Andy", "age":30}
{"name":"Justin", "age":19}
```

### Copied to Container
Command from host terminal:
```bash
docker cp people.json da-spark-master:/opt/spark/data/book_data/people.json
```
---

## Start Spark Shell

```bash
docker exec -it da-spark-master /bin/bash
```
```bash
spark-shell
```
**output**
```
Setting default log level to "WARN".
To adjust logging level use sc.setLogLevel(newLevel). For SparkR, use setLogLevel(newLevel).
25/05/31 14:17:11 WARN NativeCodeLoader: Unable to load native-hadoop library for your platform... using builtin-java classes where applicable
25/05/31 14:17:13 WARN Utils: Service 'SparkUI' could not bind on port 4040. Attempting port 4041.
Spark context Web UI available at http://05bf8e5b0dcc:4041
Spark context available as 'sc' (master = spark://spark-master:7077, app id = app-20250531141713-0005).
Spark session available as 'spark'.
Welcome to
      ____              __
     / __/__  ___ _____/ /__
    _\ \/ _ \/ _ `/ __/  '_/
   /___/ .__/\_,_/_/ /_/\_\   version 3.3.3
      /_/
         
Using Scala version 2.12.15 (OpenJDK 64-Bit Server VM, Java 11.0.27)
Type in expressions to have them evaluated.
Type :help for more information.

scala> :quit
```
---

## Initialize Spark Session

```scala
import org.apache.spark.sql.SparkSession
val spark = org.apache.spark.sql.SparkSession.builder()
  .appName("Spark SQL Example")
  .getOrCreate()

import spark.implicits._
```
**output**
```
spark: org.apache.spark.sql.SparkSession.Builder = org.apache.spark.sql.SparkSession$Builder@1938f3b3
```
---

## 4. Load the JSON File

```scala
val df = spark.read.json("/opt/spark/data/book_data/people.json")
df.show()
```

**Output:**
```
+-------+----+
|   name| age|
+-------+----+
|Michael|null|
|   Andy|  30|
| Justin|  19|
+-------+----+
```

---

##  Print Schema

```scala
df.printSchema()
```

**Output:**
```
root
 |-- age: long (nullable = true)
 |-- name: string (nullable = true)
```

---

## DataFrame Operations

### Select Name Column
```scala
df.select("name").show()
```

### Filter Age > 21
```scala
df.filter($"age" > 21).show()
```

### Group by Age
```scala
df.groupBy("age").count().show()
```

**Output:**
```
+----+-----+
| age|count|
+----+-----+
|  19|    1|
|null|    1|
|  30|    1|
+----+-----+
```

---

## Temporary View & SQL Query

```scala
df.createOrReplaceTempView("people")
val sqlDF = spark.sql("SELECT * FROM people")
sqlDF.show()
```

**Output:**
```
+-------+----+
|   name| age|
+-------+----+
|Michael|null|
|   Andy|  30|
| Justin|  19|
+-------+----+
```

---

## Global Temporary View

```scala
df.createGlobalTempView("people")
spark.sql("SELECT * FROM global_temp.people").show()
```

**Output:** (same as above)

---

## Observations

- One record had a missing `age`, which Spark interpreted as `null`.
- `groupBy("age")` includes `null` as a grouping key.
- Global temp views require prefixing with `global_temp.`.

---

## 9. Notes

- All Spark commands were executed in `spark-shell` inside the Docker container.
- JSON data was injected using `docker cp`.
- The environment was fully local, no Hadoop or HDFS used.


# Exercise 02

## Json file creation `addresses.json`

```json
{"name":"George", "address":"Main Street 2", "city":"Boston"}
{"name":"Andy", "address":"Secondary Road, 1", "city":"London"}
{"name":"Justin", "address":"Round Square, 0", "city":"Madrid"}
{"name":"Hannah", "address":"Abbey Road, 9", "city":"London"}
{"name":"Leah", "address":"Penny Lane, 3", "city":"Liverpool"} 
```

I used this command, and coppied the above json in it
```bash
nano addresses.json
```
## Move the json to the container 
```bash
docker cp addresses.json da-spark-master:/opt/spark/data/boo
k_data/addresses.json
```
**and the output is:**
```
Successfully copied 2.05kB to da-spark-master:/opt/spark/data/book_data/addresses.json
```
## start spark shell
```bash
docker exec da-spark-master spark-shell
```
## Read the addresses file into a new Data Frame df2.

```scala
val df2=spark.read.json("/opt/spark/data/book_data/addresses.json")
```
**output**
```
df2: org.apache.spark.sql.DataFrame = [address: string, city: string ... 1 more field]
```

```
scala> df2.show()
+-----------------+---------+------+                                            
|          address|     city|  name|
+-----------------+---------+------+
|    Main Street 2|   Boston|George|
|Secondary Road, 1|   London|  Andy|
|  Round Square, 0|   Madrid|Justin|
|    Abbey Road, 9|   London|Hannah|
|    Penny Lane, 3|Liverpool|  Leah|
+-----------------+---------+------+
```
```
scala> df2.printSchema()
root
 |-- address: string (nullable = true)
 |-- city: string (nullable = true)
 |-- name: string (nullable = true)
 ```

## domain-specific syntax to operate on Data Frames
 **inner Join**

 ```

scala> val df3=df1.join(df2,df1("name")===df2("name"),"inner")
df3: org.apache.spark.sql.DataFrame = [age: bigint, name: string ... 3 more fields]

scala> df3.show()
+---+------+-----------------+------+------+                                    
|age|  name|          address|  city|  name|
+---+------+-----------------+------+------+
| 30|  Andy|Secondary Road, 1|London|  Andy|
| 19|Justin|  Round Square, 0|Madrid|Justin|
+---+------+-----------------+------+------+
```
**Outer Join**
```
scala> val df4=df1.join(df2,df1("name")===df2("name"),"full")
df4: org.apache.spark.sql.DataFrame = [age: bigint, name: string ... 3 more fields]

scala> df4.show()
+----+-------+-----------------+---------+------+                               
| age|   name|          address|     city|  name|
+----+-------+-----------------+---------+------+
|  30|   Andy|Secondary Road, 1|   London|  Andy|
|null|   null|    Main Street 2|   Boston|George|
|null|   null|    Abbey Road, 9|   London|Hannah|
|  19| Justin|  Round Square, 0|   Madrid|Justin|
|null|   null|    Penny Lane, 3|Liverpool|  Leah|
|null|Michael|             null|     null|  null|
+----+-------+-----------------+---------+------+

```

## Using the SQL syntax

```scala
scala> df2.createOrReplaceTempView("df2")

scala> df1.createOrReplaceTempView("df1")

scala> val sqlDF = spark.sql("""
     |   SELECT * FROM df1
     |   FULL OUTER JOIN df2 ON df1.name = df2.name
     | """)
```
**output**
```
sqlDF: org.apache.spark.sql.DataFrame = [age: bigint, name: string ... 3 more fields]

scala> sqlDF.show()
+----+-------+-----------------+---------+------+                               
| age|   name|          address|     city|  name|
+----+-------+-----------------+---------+------+
|  30|   Andy|Secondary Road, 1|   London|  Andy|
|null|   null|    Main Street 2|   Boston|George|
|null|   null|    Abbey Road, 9|   London|Hannah|
|  19| Justin|  Round Square, 0|   Madrid|Justin|
|null|   null|    Penny Lane, 3|Liverpool|  Leah|
|null|Michael|             null|     null|  null|
+----+-------+-----------------+---------+------+
```
