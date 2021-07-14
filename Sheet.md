
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



