### Task 1
Get emails of converted users from transactions
- Select **`email`** column in **`salesDF`** and remove duplicates
- Add new column **`converted`** with value **`True`** for all rows

```Scala
val convertedUsersDF = salesDF.select("email").distinct()
                                              .withColumn("converted", lit(true))
```


### Task 2
Join emails with user IDs
- Perform an outer join on **`convertedUsersDF`** and **`usersDF`** with the **`email`** field
- Filter for users where **`email`** is not null
- Fill null values in **`converted`** as **`False`**

```Scala
val conversionsDF = usersDF.join(convertedUsersDF, Seq("email"), "outer")
  .filter(col("email").isNotNull)
  .na.fill(false)
```

### Task 3
Get cart item history for each user
- Explode **`items`** field in **`eventsDF`**
- Group by **`user_id`**
  - Collect set of all **`items.item_id`** objects for each user and alias with "cart"

```Scala
val cartsDF = eventsDF.withColumn("items", explode(col("items")))
                      .groupBy("user_id")
                      .agg(collect_set("items.item_id").alias("cart"))
```


### Task 4
Filter for emails with abandoned cart items
- Filter **`emailCartsDF`** for users where **`converted`** is False
- Filter for users with non-null carts

```Scala
emailCartsDF.filter(col("converted") === false && col("cart").isNotNull)
```

### Task 5
Plot number of abandoned cart items by product

```Scala
val abandonedItemsDF = abandonedCartsDF.withColumn("items", explode(col("cart")))
  .groupBy("items").count()
  .sort("items")
```




