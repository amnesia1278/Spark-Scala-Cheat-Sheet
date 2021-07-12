
# DataFrame & Column

## 1. Construct columns

```Scala
```

### 1.1 Access a column
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

### 1.2 Subset columns -> select(), selectExpr(), drop()

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

