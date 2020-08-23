# DotNet.DynamoDB.Utilities
Utilities for working with DynamoDB in .Net

## Paging
The paging utilities provide the following extension methods for `Amazon.DynamoDBv2.IAmazonDynamoDB`.

### QueryAllAsync
This method will return all records for a query request (`Amazon.DynamoDBv2.Model.QueryRequest`), taking care of the paging automatically.

> NOTE: the ExclusiveStartKey and Limit values of the queryRequest will be overridden by this method.

> CAUTION: this method will return all values without concern for memory

#### Usage

``` C#
var dyanmoDB = new Amazon.DynamoDBv2.AmazonDynamoDBClient();

var queryRequest = new QueryRequest
{
    KeyConditionExpression = "pk = :pk AND begins_with(sk, :sk)",
    ExpressionAttributeValues = new Dictionary<string, AttributeValue>
    {
        { ":pk", new AttributeValue("partition") },
        { ":sk", new AttributeValue("sort|") },
    },
    TableName = tableName
};

List<Dictionary<string, AttributeValue>> allItems = await dynamoDB.QueryAllAsync(queryRequest);
```

### QueryPageAsync
This method provides a paging abstraction on the standard DynamoDB paging. It can be useful when working with large sets of data where you need to remove data from memory but still need the ability to move in both directions.
The result ([PageResult](./src/DynamoDB.Utilities/Model/PageResult.cs)) includes the page data (`List<Dictionary<string, AttributeValue>>`) as well as page info that includes keys for going forward or backward through the data that is Base64 encoded so it can be returned to a client (e.g. web or mobile app). Imagine the following data:
```
A
B
C
D
E
F
G
H
```

If paging in ascending [SortOrder](./src/DynamoDB.Utilities/Model/SortOrder.cs) with a limit of two (2) then the first request (`null` as the `pageKey` value) should return records `A` and `B`. The result will include a `ForwardKey` that can be used for the next request. That request would return records `C` and `D`. The result of that request includes keys to go forward (to get records `E` and `F`) or to go backwards, using the `ReverseKey` (to get records `A` and `B`).

If paging in descending [SortOrder](./src/DynamoDB.Utilities/Model/SortOrder.cs) with a limit of two (2) then the first request (`null` as the `pageKey` value) should return records `H` and `G`. The result will include a `ForwardKey` that can be used for the next request. That request would return records `F` and `E`. The result of that request includes keys to go forward (to get records `D` and `CF`) or to go backwards, using the `ReverseKey` (to get records `H` and `G`).

#### Usage

``` C#
var dyanmoDB = new Amazon.DynamoDBv2.AmazonDynamoDBClient();

var queryRequest = new QueryRequest
{
    KeyConditionExpression = "gsi1_pk = :pk AND begins_with(gsi1_sk, :sk)",
    ExpressionAttributeValues = new Dictionary<string, AttributeValue>
    {
        { ":pk", new AttributeValue("partition") },
        { ":sk", new AttributeValue("sort|") },
    },
    TableName = tableName,
    IndexName = "gsi1"
};

// this contains information about the key structure of the table
// it is used to construct and deconstruct the paging keys
// the table keys (PartitionKeyName and SortKeyName must always match the keys for the table)
// the index keys must match the index being queried (null if not querying an index)
var keyInfo = new PageKeyInfo
{
    PartitionKeyName = "pk",
    SortKeyName = "sk",
    IndexPartitionKeyName = "gsi1_pk",
    IndexSortKeyName = "gsi1_sk"
};

var pageResult = await dynamoDB.QueryPageAsync(queryRequest, null, SortOrder.Ascending, pageKeyInfo);

while (pageResult.PageInfo.ForwardKey != null)
{
    pageResult = await dynamoDB.QueryPageAsync(queryRequest, pageResult.PageInfo.ForwardKey, SortOrder.Ascending, pageKeyInfo);
}
```
