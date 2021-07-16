
# DataFrame & Column

## 1. Construct columns

## 1.1 Access a column
```Scala
import org.apache.spark.sql.functions.col

col("device")
$"device"
eventsDF("device")
```

### 1.1.1 Example with column expression

```Scala
val revDF = eventsDF.filter($"ecommerce.purchase_revenue_in_usd".isNotNull)
  .withColumn("purchase_revenue", ($"ecommerce.purchase_revenue_in_usd" * 100).cast("int"))
  .withColumn("avg_purchase_revenue", $"ecommerce.purchase_revenue_in_usd" / $"ecommerce.total_item_quantity")
  .sort($"avg_purchase_revenue".desc)
```

## 1.2 Subset columns -> select(), selectExpr(), drop()

### 1.2.1 select() Selects a set of columns or column based expressions
```Scala
//Example 1
val devicesDF = eventsDF.select("user_id", "device")
display(devicesDF)

//Example 2
val locationsDF = eventsDF.select(col("user_id"),
  col("geo.city").as("city"),
  col("geo.state").as("state"))
```
### 1.2.2 selectExpr() Selects a set of SQL expressions

```Scala
val appleDF = eventsDF.selectExpr("user_id", "device in ('macOS', 'iOS') as apple_user")
```

### 1.2.3 drop()
```Scala
val anonymousDF = eventsDF.drop("user_id", "geo", "device")
```

## 1.3 Add or replace columns -> withColumn, withColumnRenamed()

### 1.3.1 withColumn

```Scala
val mobileDF = eventsDF.withColumn("mobile", col("device").isin("iOS", "Android"))
```
### 1.3.2 withColumnRenamed

```Scala
val locationDF = eventsDF.withColumnRenamed("geo", "location")
```

## 2. Aggregation

### 2.1 GroupBy

```Scala
df.groupBy("event_name")
// or
df.groupBy("geo.state", "geo.city")
```

### 2.2 GroupBy with methods (alias are not possible)

```Scala
val eventCountsDF = df.groupBy("event_name").count()
// or
val avgStatePurchasesDF = df.groupBy("geo.state").avg("ecommerce.purchase_revenue_in_usd")
```

### 2.3 Built-in aggregate functions (alias are possible)

```Scala
val statePurchasesDF = df.groupBy("geo.state").agg(sum("ecommerce.total_item_quantity").alias("total_purchases"))
// or
val stateAggregatesDF = df.groupBy("geo.state").agg(
  avg("ecommerce.total_item_quantity").alias("avg_quantity"),
  approx_count_distinct("user_id").alias("distinct_users"))
```

## 3. Datetime Functions

### 3.1 cast() -> Casts column to a different data type, specified using string representation or DataType.

```Scala
val timestampDF = df.withColumn("timestamp", (col("timestamp") / 1e6).cast("timestamp"))
// or
val timestampDF = df.withColumn("timestamp", (col("timestamp") / 1e6).cast(TimestampType))
```

### 3.2 Format date

### 3.2.1 date_format() -> Converts a date/timestamp/string to a string formatted with the given date time pattern.
valid date and time format patterns for <a href="https://spark.apache.org/docs/latest/sql-ref-datetime-pattern.html" target="_blank">Spark 3</a> and <a href="https://docs.oracle.com/javase/8/docs/api/java/text/SimpleDateFormat.html" target="_blank">Spark 2</a>.
```Scala
val formattedDF = timestampDF.withColumn("date string", date_format(col("timestamp"), "MMMM dd, yyyy"))
  .withColumn("time string", date_format(col("timestamp"), "HH:mm:ss.SSSSSS"))
```

### 3.2.2 Extract datetime attribute from timestamp

```Scala
val datetimeDF = timestampDF.withColumn("year", year(col("timestamp")))
  .withColumn("month", month(col("timestamp")))
  .withColumn("dayofweek", dayofweek(col("timestamp")))
  .withColumn("minute", minute(col("timestamp")))
  .withColumn("second", second(col("timestamp")))
```

### 3.2.3 Convert to Date

```Scala
val dateDF = timestampDF.withColumn("date", to_date(col("timestamp")))
```


### 3.2.4 Manipulate Datetimes

Example: date_add

```Scala
val plus2DF = timestampDF.withColumn("plus_two_days", date_add(col("timestamp"), 2))
```

## 4. Complex types

### 4.1 Extract item details

```Scala
val detailsDF = (df.withColumn("items", explode(col("items"))) // выделяет каждый элемент сложного типа в отдельную колонку
  .select("email", "items.item_name")
  .withColumn("details", split(col("item_name"), " ")) // разделает строку на элементы по пробелу Standard Full Mattress -> ["Standard", "Full", "Mattress"]
)
```

### 4.2 Extract size and quality options from purchases

```Scala
val mattressDF = detailsDF.filter(array_contains(col("details"), "Mattress")) //Filter detailsDF for records where details contains "Mattress"
  .withColumn("size", element_at(col("details"), 2)) //Add size column from extracting element at position 2
  .withColumn("quality", element_at(col("details"), 1)) //Add quality column from extracting element at position 1
)
// or same for pillows

val pillowDF = detailsDF.filter(array_contains(col("details"), "Pillow"))
  .withColumn("size", element_at(col("details"), 1))
  .withColumn("quality", element_at(col("details"), 2))
```

### 4.3 Extract size and quality options from purchases

```Scala
val mattressDF = detailsDF.filter(array_contains(col("details"), "Mattress")) //Filter detailsDF for records where details contains "Mattress"
  .withColumn("size", element_at(col("details"), 2)) //Add size column from extracting element at position 2
  .withColumn("quality", element_at(col("details"), 1)) //Add quality column from extracting element at position 1
)
```

### 4.4 Combine data for mattress and pillows (**`not sure its right`**)

```Scala
val unionDF = mattressDF.unionByName(pillowDF)
  .drop("details")
)
```


### 4.5 List all size and quality options bought by each user

```Scala
val optionsDF = unionDF.groupBy("email") //Group rows in unionDF by email
  .agg(collect_set("size").alias("size options"), //Collect set of all items in size for each user with alias "size options"
       collect_set("quality").alias("quality options")) //Collect set of all items in quality for each user with alias "quality options"
)
```


## UDF 

```Scala
// Step 1 - define a function

def firstLetterFunction (email: String): String = {
  email(0).toString
}
)

// Step 2 assign our defined function to the variable

val firstLetterUDF = udf(firstLetterFunction _)

// Step now we can apply our function

salesDF.select(firstLetterUDF(col("email")))

```

### Register function to the SQL

```Scala
// Register
spark.udf.register("sql_udf", firstLetterFunction _)
```

```Scala
// Apply
%sql
SELECT sql_udf(email) AS firstLetter FROM sales
```




