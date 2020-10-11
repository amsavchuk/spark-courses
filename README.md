# What was done
Task 1 was implemented using Spark SQL and custom aggregator (UDAF functions are deprecated in new Spark 3.0).
All parts in task 2 were implemented on Spark SQL on top of previous task. 
Also an additional alternative implementation of the task 2 was developed 
by using Spark Scala DataFrame API only (without Spark SQL).
All solutions were covered by unit and integration tests.
 
# Task 1
Steps for solution:
1. Convert `attributes` column in mobile app click stream from `String` to `Option[Map[String, String]]`.
It is done by using `from_json` function;
2. Apply the custom aggregator as window function (with partitioning by `userId`) in order to calculate `List[purchaseId]` with `campaingId` and `channelId`;
3. Group by `userId` and choose one row from each group by applying aggregation function `first` 
(because all rows are equal it is needed to choose only one);
4. Flat result of aggregation by using built-it function `inline`;
It explodes an array of structs into a table. Uses field names of struct as new columns;
5. Generate `UUID` for each value in `sessionId` column;
6. Explode `purchaseIds` which has type of `Array[PurchaseId]` into multiple rows;
6. Join with `purchases` table.
  
# Task 2

## Task 2.1
Steps for solution:
1. Filter by `isConfirmed` column;
2. Group by `campaignId` and sum up `billingCost`;
3. Sort by sum of `billingCost` in descending order;
4. Choose only top 10 campaigns. 

## Task 2.2
Steps for solution:
1. Convert `attributes` column in mobile app click stream from `String` to `Option[Map[String, String]]`;
2. Expand `attributes` into two columns: `campagnId` and `channelId`;
3. Group by `campagnId` and `channelId` in order to calculate unique sessions;
3. Group by `campaignId` and using custom aggregator to find the top `channelId` in each group associated 
with `campaignId`.
