using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Amazon.DynamoDBv2.Model;
using FluentAssertions;
using Wadsworth.DynamoDB.Utilities;
using Wadsworth.DynamoDB.Utilities.Model;
using Xunit;

namespace DynamoDB.Utilities.Tests.Paging
{
    [Collection("TestCollection")]
    public class GetPageAsync
    {
        private readonly TestFixture testFixture;

        public GetPageAsync(TestFixture testFixture)
        {
            this.testFixture = testFixture;
        }

        [Fact(DisplayName = "Paging.GetAllAsync - Test w/o Index Ascending")]
        public async Task TestNoIndexAscending()
        {
            var count = 25;
            var total = ((count * 2) + 1);
            var tableName = "paging-with-sk";
            await testFixture.CreateTablesAsync();

            var values = new List<int>();
            for (var x = 0; x < total; x++)
            {
                values.Add(x);
                await testFixture.AmazonDynamoDB.PutItemAsync(tableName, new Dictionary<string, AttributeValue>
                {
                    { "pk", new AttributeValue("partition") },
                    { "sk", new AttributeValue($"sort|{x.ToString("000")}") },
                    { "x", new AttributeValue { N = x.ToString() } },
                });
            }

            var queryRequest = new QueryRequest
            {
                KeyConditionExpression = "pk = :pk AND begins_with(sk, :sk)",
                ExpressionAttributeValues = new Dictionary<string, AttributeValue>
                {
                    { ":pk", new AttributeValue("partition") },
                    { ":sk", new AttributeValue("sort|") },
                },
                TableName = tableName,
                Limit = count
            };

            var keyInfo = new PageKeyInfo { PartitionKeyName = "pk", SortKeyName = "sk" };

            // null should return the first 25 records
            var result = await testFixture.AmazonDynamoDB.QueryPageAsync(queryRequest, null, SortOrder.Ascending, keyInfo);

            result.Should().NotBeNull();
            result.PageInfo.Should().NotBeNull();
            result.PageInfo.ReverseKey.Should().NotBeNull();
            result.PageInfo.ForwardKey.Should().NotBeNull();
            result.Data.Count.Should().Be(count);
            result.Data.Select(d => int.Parse(d["x"].N)).Should().BeEquivalentTo(GetOrderedList(0, count));

            // using the `ForwardKey` from the previous request should get us the next 25 records
            result = await testFixture.AmazonDynamoDB.QueryPageAsync(queryRequest, result.PageInfo.ForwardKey, SortOrder.Ascending, keyInfo);

            result.Should().NotBeNull();
            result.PageInfo.Should().NotBeNull();
            result.PageInfo.ReverseKey.Should().NotBeNull();
            result.PageInfo.ForwardKey.Should().NotBeNull();
            result.Data.Count.Should().Be(count);
            result.Data.Select(d => int.Parse(d["x"].N)).Should().BeEquivalentTo(GetOrderedList(count, count * 2));

            // using the `ReverseKey` from the previous request, should give us the same records as the first request
            result = await testFixture.AmazonDynamoDB.QueryPageAsync(queryRequest, result.PageInfo.ReverseKey, SortOrder.Ascending, keyInfo);

            result.Should().NotBeNull();
            result.PageInfo.Should().NotBeNull();
            result.PageInfo.ReverseKey.Should().NotBeNull();
            result.PageInfo.ForwardKey.Should().NotBeNull();
            result.Data.Count.Should().Be(count);
            result.Data.Select(d => int.Parse(d["x"].N)).Should().BeEquivalentTo(GetOrderedList(0, count));

            // using the `ReverseKey` from the previous request, should give us no records and null keys (we are at the beginning)
            result = await testFixture.AmazonDynamoDB.QueryPageAsync(queryRequest, result.PageInfo.ReverseKey, SortOrder.Ascending, keyInfo);

            result.Should().NotBeNull();
            result.PageInfo.Should().NotBeNull();
            result.PageInfo.ReverseKey.Should().BeNull();
            result.PageInfo.ForwardKey.Should().BeNull();
            result.Data.Count.Should().Be(0);

            // start forward again, but go to the third page
            // null should return the first 25 records
            result = await testFixture.AmazonDynamoDB.QueryPageAsync(queryRequest, null, SortOrder.Ascending, keyInfo);
            result = await testFixture.AmazonDynamoDB.QueryPageAsync(queryRequest, result.PageInfo.ForwardKey, SortOrder.Ascending, keyInfo);

            // this should give us the last page, and because the last record was less than the limit, we will receive a null `EndKey`, indicating we've reached the end
            result = await testFixture.AmazonDynamoDB.QueryPageAsync(queryRequest, result.PageInfo.ForwardKey, SortOrder.Ascending, keyInfo);

            result.Should().NotBeNull();
            result.PageInfo.Should().NotBeNull();
            result.PageInfo.ReverseKey.Should().NotBeNull();
            result.PageInfo.ForwardKey.Should().BeNull();
            result.Data.Count.Should().Be(1);
            result.Data.Select(d => int.Parse(d["x"].N)).Should().BeEquivalentTo(new List<int> { total - 1 });
        }

        [Fact(DisplayName = "Paging.GetAllAsync - Test w/o Index Ascending w/ Record Count Matching Limit")]
        public async Task TestNoIndexAscendingExactDataSize()
        {
            var count = 25;
            var tableName = "paging-with-sk";
            await testFixture.CreateTablesAsync();

            var values = new List<int>();
            for (var x = 0; x < count; x++)
            {
                values.Add(x);
                await testFixture.AmazonDynamoDB.PutItemAsync(tableName, new Dictionary<string, AttributeValue>
                {
                    { "pk", new AttributeValue("partition") },
                    { "sk", new AttributeValue($"sort|{x.ToString("000")}") },
                    { "x", new AttributeValue { N = x.ToString() } },
                });
            }

            var queryRequest = new QueryRequest
            {
                KeyConditionExpression = "pk = :pk AND begins_with(sk, :sk)",
                ExpressionAttributeValues = new Dictionary<string, AttributeValue>
                {
                    { ":pk", new AttributeValue("partition") },
                    { ":sk", new AttributeValue("sort|") },
                },
                TableName = tableName,
                Limit = count
            };

            var keyInfo = new PageKeyInfo { PartitionKeyName = "pk", SortKeyName = "sk" };

            // null should return the first 25 records
            var result = await testFixture.AmazonDynamoDB.QueryPageAsync(queryRequest, null, SortOrder.Ascending, keyInfo);

            result.Should().NotBeNull();
            result.PageInfo.Should().NotBeNull();
            result.PageInfo.ReverseKey.Should().NotBeNull();
            result.PageInfo.ForwardKey.Should().NotBeNull();
            result.Data.Count.Should().Be(count);
            result.Data.Select(d => int.Parse(d["x"].N)).Should().BeEquivalentTo(GetOrderedList(0, count));

            // using the `ForwardKey` from the previous request should get us no records and empty keys (we are at the end)
            result = await testFixture.AmazonDynamoDB.QueryPageAsync(queryRequest, result.PageInfo.ForwardKey, SortOrder.Ascending, keyInfo);

            result.Should().NotBeNull();
            result.PageInfo.Should().NotBeNull();
            result.PageInfo.ReverseKey.Should().BeNull();
            result.PageInfo.ForwardKey.Should().BeNull();
            result.Data.Count.Should().Be(0);
        }

        [Fact(DisplayName = "Paging.GetAllAsync - Test w/o Index Descending")]
        public async Task TestNoIndexDescending()
        {
            var count = 25;
            var total = ((count * 2) + 1);
            var tableName = "paging-with-sk";
            await testFixture.CreateTablesAsync();

            var values = new List<int>();
            for (var x = 0; x < total; x++)
            {
                values.Add(x);
                await testFixture.AmazonDynamoDB.PutItemAsync(tableName, new Dictionary<string, AttributeValue>
                {
                    { "pk", new AttributeValue("partition") },
                    { "sk", new AttributeValue($"sort|{x.ToString("000")}") },
                    { "x", new AttributeValue { N = x.ToString() } },
                });
            }

            var queryRequest = new QueryRequest
            {
                KeyConditionExpression = "pk = :pk AND begins_with(sk, :sk)",
                ExpressionAttributeValues = new Dictionary<string, AttributeValue>
                {
                    { ":pk", new AttributeValue("partition") },
                    { ":sk", new AttributeValue("sort|") },
                },
                TableName = tableName,
                Limit = count
            };

            var keyInfo = new PageKeyInfo { PartitionKeyName = "pk", SortKeyName = "sk" };

            // null should return the last 25 records
            var result = await testFixture.AmazonDynamoDB.QueryPageAsync(queryRequest, null, SortOrder.Decending, keyInfo);

            result.Should().NotBeNull();
            result.PageInfo.Should().NotBeNull();
            result.PageInfo.ReverseKey.Should().NotBeNull();
            result.PageInfo.ForwardKey.Should().NotBeNull();
            result.Data.Count.Should().Be(count);
            result.Data.Select(d => int.Parse(d["x"].N)).Should().BeEquivalentTo(GetOrderedList(total, total - count));

            // using the `ForwardKey` from the previous request should get us the next 25 records in reverse
            result = await testFixture.AmazonDynamoDB.QueryPageAsync(queryRequest, result.PageInfo.ForwardKey, SortOrder.Decending, keyInfo);

            result.Should().NotBeNull();
            result.PageInfo.Should().NotBeNull();
            result.PageInfo.ReverseKey.Should().NotBeNull();
            result.PageInfo.ForwardKey.Should().NotBeNull();
            result.Data.Count.Should().Be(count);
            result.Data.Select(d => int.Parse(d["x"].N)).Should().BeEquivalentTo(GetOrderedList(total - count, total - count - count));

            // using the `ReverseKey` from the previous request, should give us the same records as the first request
            result = await testFixture.AmazonDynamoDB.QueryPageAsync(queryRequest, result.PageInfo.ReverseKey, SortOrder.Decending, keyInfo);

            result.Should().NotBeNull();
            result.PageInfo.Should().NotBeNull();
            result.PageInfo.ReverseKey.Should().NotBeNull();
            result.PageInfo.ForwardKey.Should().NotBeNull();
            result.Data.Count.Should().Be(count);
            result.Data.Select(d => int.Parse(d["x"].N)).Should().BeEquivalentTo(GetOrderedList(total, total - count));

            // using the `ReverseKey` from the previous request, should give us no records and null keys (we are at the "beginning")
            result = await testFixture.AmazonDynamoDB.QueryPageAsync(queryRequest, result.PageInfo.ReverseKey, SortOrder.Decending, keyInfo);

            result.Should().NotBeNull();
            result.PageInfo.Should().NotBeNull();
            result.PageInfo.ReverseKey.Should().BeNull();
            result.PageInfo.ForwardKey.Should().BeNull();
            result.Data.Count.Should().Be(0);

            // start forward again, but go to the third page
            // null should return the first 25 records
            result = await testFixture.AmazonDynamoDB.QueryPageAsync(queryRequest, null, SortOrder.Decending, keyInfo);
            result = await testFixture.AmazonDynamoDB.QueryPageAsync(queryRequest, result.PageInfo.ForwardKey, SortOrder.Decending, keyInfo);

            // this should give us the last page, and because the last record was less than the limit, we will receive a null `EndKey`, indicating we've reached the end
            result = await testFixture.AmazonDynamoDB.QueryPageAsync(queryRequest, result.PageInfo.ForwardKey, SortOrder.Decending, keyInfo);

            result.Should().NotBeNull();
            result.PageInfo.Should().NotBeNull();
            result.PageInfo.ReverseKey.Should().NotBeNull();
            result.PageInfo.ForwardKey.Should().BeNull();
            result.Data.Count.Should().Be(1);
            result.Data.Select(d => int.Parse(d["x"].N)).Should().BeEquivalentTo(new List<int> { 0 });
        }

        [Fact(DisplayName = "Paging.GetAllAsync - Test w/ Index Ascending")]
        public async Task TestIndexAscending()
        {
            var count = 25;
            var total = ((count * 4) + 1);
            var tableName = "paging-with-sk";
            await testFixture.CreateTablesAsync();

            var values = new List<int>();
            for (var x = 0; x < total; x++)
            {
                values.Add(x);
                await testFixture.AmazonDynamoDB.PutItemAsync(tableName, new Dictionary<string, AttributeValue>
                {
                    { "pk", new AttributeValue("partition") },
                    { "sk", new AttributeValue($"sort|{x.ToString("000")}") },
                    { "gsi1_pk", new AttributeValue($"index|{(x % 2).ToString("000")}") },
                    { "gsi1_sk", new AttributeValue($"sort|{x.ToString("000")}") },
                    { "x", new AttributeValue { N = x.ToString() } },
                    { "modx", new AttributeValue { N = (x % 2).ToString() } },
                });
            }

            var queryRequest = new QueryRequest
            {
                KeyConditionExpression = "gsi1_pk = :pk AND begins_with(gsi1_sk, :sk)",
                ExpressionAttributeValues = new Dictionary<string, AttributeValue>
                {
                    { ":pk", new AttributeValue("index|000") },
                    { ":sk", new AttributeValue("sort|") },
                },
                TableName = tableName,
                IndexName = "gsi1",
                Limit = count
            };

            var keyInfo = new PageKeyInfo { PartitionKeyName = "pk", SortKeyName = "sk", IndexPartitionKeyName = "gsi1_pk", IndexSortKeyName = "gsi1_sk" };

            // null should return the first 25 records
            var result = await testFixture.AmazonDynamoDB.QueryPageAsync(queryRequest, null, SortOrder.Ascending, keyInfo);

            result.Should().NotBeNull();
            result.PageInfo.Should().NotBeNull();
            result.PageInfo.ReverseKey.Should().NotBeNull();
            result.PageInfo.ForwardKey.Should().NotBeNull();
            result.Data.Count.Should().Be(count);
            result.Data.Select(d => int.Parse(d["x"].N)).Should().BeEquivalentTo(GetOrderedList(0, count * 2, 2));

            // using the `ForwardKey` from the previous request should get us the next 25 records
            result = await testFixture.AmazonDynamoDB.QueryPageAsync(queryRequest, result.PageInfo.ForwardKey, SortOrder.Ascending, keyInfo);

            result.Should().NotBeNull();
            result.PageInfo.Should().NotBeNull();
            result.PageInfo.ReverseKey.Should().NotBeNull();
            result.PageInfo.ForwardKey.Should().NotBeNull();
            result.Data.Count.Should().Be(count);
            result.Data.Select(d => int.Parse(d["x"].N)).Should().BeEquivalentTo(GetOrderedList(count * 2, count * 4, 2));

            // using the `ReverseKey` from the previous request, should give us the same records as the first request
            result = await testFixture.AmazonDynamoDB.QueryPageAsync(queryRequest, result.PageInfo.ReverseKey, SortOrder.Ascending, keyInfo);

            result.Should().NotBeNull();
            result.PageInfo.Should().NotBeNull();
            result.PageInfo.ReverseKey.Should().NotBeNull();
            result.PageInfo.ForwardKey.Should().NotBeNull();
            result.Data.Count.Should().Be(count);
            result.Data.Select(d => int.Parse(d["x"].N)).Should().BeEquivalentTo(GetOrderedList(0, count * 2, 2));

            // using the `ReverseKey` from the previous request, should give us no records and null keys (we are at the beginning)
            result = await testFixture.AmazonDynamoDB.QueryPageAsync(queryRequest, result.PageInfo.ReverseKey, SortOrder.Ascending, keyInfo);

            result.Should().NotBeNull();
            result.PageInfo.Should().NotBeNull();
            result.PageInfo.ReverseKey.Should().BeNull();
            result.PageInfo.ForwardKey.Should().BeNull();
            result.Data.Count.Should().Be(0);

            // start forward again, but go to the third page
            // null should return the first 25 records
            result = await testFixture.AmazonDynamoDB.QueryPageAsync(queryRequest, null, SortOrder.Ascending, keyInfo);
            result = await testFixture.AmazonDynamoDB.QueryPageAsync(queryRequest, result.PageInfo.ForwardKey, SortOrder.Ascending, keyInfo);

            // this should give us the last page, and because the last record was less than the limit, we will receive a null `EndKey`, indicating we've reached the end
            result = await testFixture.AmazonDynamoDB.QueryPageAsync(queryRequest, result.PageInfo.ForwardKey, SortOrder.Ascending, keyInfo);

            result.Should().NotBeNull();
            result.PageInfo.Should().NotBeNull();
            result.PageInfo.ReverseKey.Should().NotBeNull();
            result.PageInfo.ForwardKey.Should().BeNull();
            result.Data.Count.Should().Be(1);
            result.Data.Select(d => int.Parse(d["x"].N)).Should().BeEquivalentTo(new List<int> { total - 1 });
        }

        [Fact(DisplayName = "Paging.GetAllAsync - Test w/ Index Descending")]
        public async Task TestIndexDescending()
        {
            var count = 25;
            var total = ((count * 4) + 1);
            var tableName = "paging-with-sk";
            await testFixture.CreateTablesAsync();

            var values = new List<int>();
            for (var x = 0; x < total; x++)
            {
                values.Add(x);
                await testFixture.AmazonDynamoDB.PutItemAsync(tableName, new Dictionary<string, AttributeValue>
                {
                    { "pk", new AttributeValue("partition") },
                    { "sk", new AttributeValue($"sort|{x.ToString("000")}") },
                    { "gsi1_pk", new AttributeValue($"index|{(x % 2).ToString("000")}") },
                    { "gsi1_sk", new AttributeValue($"sort|{x.ToString("000")}") },
                    { "x", new AttributeValue { N = x.ToString() } },
                    { "modx", new AttributeValue { N = (x % 2).ToString() } },
                });
            }

            var queryRequest = new QueryRequest
            {
                KeyConditionExpression = "gsi1_pk = :pk AND begins_with(gsi1_sk, :sk)",
                ExpressionAttributeValues = new Dictionary<string, AttributeValue>
                {
                    { ":pk", new AttributeValue("index|000") },
                    { ":sk", new AttributeValue("sort|") },
                },
                TableName = tableName,
                IndexName = "gsi1",
                Limit = count
            };

            var keyInfo = new PageKeyInfo { PartitionKeyName = "pk", SortKeyName = "sk", IndexPartitionKeyName = "gsi1_pk", IndexSortKeyName = "gsi1_sk" };

            // null should return the last 25 records
            var result = await testFixture.AmazonDynamoDB.QueryPageAsync(queryRequest, null, SortOrder.Decending, keyInfo);

            result.Should().NotBeNull();
            result.PageInfo.Should().NotBeNull();
            result.PageInfo.ReverseKey.Should().NotBeNull();
            result.PageInfo.ForwardKey.Should().NotBeNull();
            result.Data.Count.Should().Be(count);
            result.Data.Select(d => int.Parse(d["x"].N)).Should().BeEquivalentTo(GetOrderedList(total, total - (count * 2), 2));

            // using the `ForwardKey` from the previous request should get us the next 25 records
            result = await testFixture.AmazonDynamoDB.QueryPageAsync(queryRequest, result.PageInfo.ForwardKey, SortOrder.Decending, keyInfo);

            result.Should().NotBeNull();
            result.PageInfo.Should().NotBeNull();
            result.PageInfo.ReverseKey.Should().NotBeNull();
            result.PageInfo.ForwardKey.Should().NotBeNull();
            result.Data.Count.Should().Be(count);
            result.Data.Select(d => int.Parse(d["x"].N)).Should().BeEquivalentTo(GetOrderedList(total - (count * 2), total - (count * 4), 2));

            // using the `ReverseKey` from the previous request, should give us the same records as the first request
            result = await testFixture.AmazonDynamoDB.QueryPageAsync(queryRequest, result.PageInfo.ReverseKey, SortOrder.Decending, keyInfo);

            result.Should().NotBeNull();
            result.PageInfo.Should().NotBeNull();
            result.PageInfo.ReverseKey.Should().NotBeNull();
            result.PageInfo.ForwardKey.Should().NotBeNull();
            result.Data.Count.Should().Be(count);
            result.Data.Select(d => int.Parse(d["x"].N)).Should().BeEquivalentTo(GetOrderedList(total, total - (count * 2), 2));

            // using the `ReverseKey` from the previous request, should give us no records and null keys (we are at the beginning)
            result = await testFixture.AmazonDynamoDB.QueryPageAsync(queryRequest, result.PageInfo.ReverseKey, SortOrder.Decending, keyInfo);

            result.Should().NotBeNull();
            result.PageInfo.Should().NotBeNull();
            result.PageInfo.ReverseKey.Should().BeNull();
            result.PageInfo.ForwardKey.Should().BeNull();
            result.Data.Count.Should().Be(0);

            // start again, but go to the third page
            result = await testFixture.AmazonDynamoDB.QueryPageAsync(queryRequest, null, SortOrder.Decending, keyInfo);
            result = await testFixture.AmazonDynamoDB.QueryPageAsync(queryRequest, result.PageInfo.ForwardKey, SortOrder.Decending, keyInfo);

            // this should give us the last page, and because the last record was less than the limit, we will receive a null `EndKey`, indicating we've reached the end
            result = await testFixture.AmazonDynamoDB.QueryPageAsync(queryRequest, result.PageInfo.ForwardKey, SortOrder.Decending, keyInfo);

            result.Should().NotBeNull();
            result.PageInfo.Should().NotBeNull();
            result.PageInfo.ReverseKey.Should().NotBeNull();
            result.PageInfo.ForwardKey.Should().BeNull();
            result.Data.Count.Should().Be(1);
            result.Data.Select(d => int.Parse(d["x"].N)).Should().BeEquivalentTo(new List<int> { 0 });
        }

        [Fact(DisplayName = "Paging.GetAllAsync - Test w/ Index w/ No Table Sort Key Ascending")]
        public async Task TestIndexNoTableSortAscending()
        {
            var count = 25;
            var total = ((count * 4) + 1);
            var tableName = "paging-without-sk";
            await testFixture.CreateTablesAsync();

            var values = new List<int>();
            for (var x = 0; x < total; x++)
            {
                values.Add(x);
                await testFixture.AmazonDynamoDB.PutItemAsync(tableName, new Dictionary<string, AttributeValue>
                {
                    { "pk", new AttributeValue($"sort|{x.ToString("000")}") },
                    { "gsi1_pk", new AttributeValue($"index|{(x % 2).ToString("000")}") },
                    { "gsi1_sk", new AttributeValue($"sort|{x.ToString("000")}") },
                    { "x", new AttributeValue { N = x.ToString() } },
                    { "modx", new AttributeValue { N = (x % 2).ToString() } },
                });
            }

            var queryRequest = new QueryRequest
            {
                KeyConditionExpression = "gsi1_pk = :pk AND begins_with(gsi1_sk, :sk)",
                ExpressionAttributeValues = new Dictionary<string, AttributeValue>
                {
                    { ":pk", new AttributeValue("index|000") },
                    { ":sk", new AttributeValue("sort|") },
                },
                TableName = tableName,
                IndexName = "gsi1",
                Limit = count
            };

            var keyInfo = new PageKeyInfo { PartitionKeyName = "pk", IndexPartitionKeyName = "gsi1_pk", IndexSortKeyName = "gsi1_sk" };

            // null should return the first 25 records
            var result = await testFixture.AmazonDynamoDB.QueryPageAsync(queryRequest, null, SortOrder.Ascending, keyInfo);

            result.Should().NotBeNull();
            result.PageInfo.Should().NotBeNull();
            result.PageInfo.ReverseKey.Should().NotBeNull();
            result.PageInfo.ForwardKey.Should().NotBeNull();
            result.Data.Count.Should().Be(count);
            result.Data.Select(d => int.Parse(d["x"].N)).Should().BeEquivalentTo(GetOrderedList(0, count * 2, 2));

            // using the `ForwardKey` from the previous request should get us the next 25 records
            result = await testFixture.AmazonDynamoDB.QueryPageAsync(queryRequest, result.PageInfo.ForwardKey, SortOrder.Ascending, keyInfo);

            result.Should().NotBeNull();
            result.PageInfo.Should().NotBeNull();
            result.PageInfo.ReverseKey.Should().NotBeNull();
            result.PageInfo.ForwardKey.Should().NotBeNull();
            result.Data.Count.Should().Be(count);
            result.Data.Select(d => int.Parse(d["x"].N)).Should().BeEquivalentTo(GetOrderedList(count * 2, count * 4, 2));

            // using the `ReverseKey` from the previous request, should give us the same records as the first request
            result = await testFixture.AmazonDynamoDB.QueryPageAsync(queryRequest, result.PageInfo.ReverseKey, SortOrder.Ascending, keyInfo);

            result.Should().NotBeNull();
            result.PageInfo.Should().NotBeNull();
            result.PageInfo.ReverseKey.Should().NotBeNull();
            result.PageInfo.ForwardKey.Should().NotBeNull();
            result.Data.Count.Should().Be(count);
            result.Data.Select(d => int.Parse(d["x"].N)).Should().BeEquivalentTo(GetOrderedList(0, count * 2, 2));

            // using the `ReverseKey` from the previous request, should give us no records and null keys (we are at the beginning)
            result = await testFixture.AmazonDynamoDB.QueryPageAsync(queryRequest, result.PageInfo.ReverseKey, SortOrder.Ascending, keyInfo);

            result.Should().NotBeNull();
            result.PageInfo.Should().NotBeNull();
            result.PageInfo.ReverseKey.Should().BeNull();
            result.PageInfo.ForwardKey.Should().BeNull();
            result.Data.Count.Should().Be(0);

            // start forward again, but go to the third page
            // null should return the first 25 records
            result = await testFixture.AmazonDynamoDB.QueryPageAsync(queryRequest, null, SortOrder.Ascending, keyInfo);
            result = await testFixture.AmazonDynamoDB.QueryPageAsync(queryRequest, result.PageInfo.ForwardKey, SortOrder.Ascending, keyInfo);

            // this should give us the last page, and because the last record was less than the limit, we will receive a null `EndKey`, indicating we've reached the end
            result = await testFixture.AmazonDynamoDB.QueryPageAsync(queryRequest, result.PageInfo.ForwardKey, SortOrder.Ascending, keyInfo);

            result.Should().NotBeNull();
            result.PageInfo.Should().NotBeNull();
            result.PageInfo.ReverseKey.Should().NotBeNull();
            result.PageInfo.ForwardKey.Should().BeNull();
            result.Data.Count.Should().Be(1);
            result.Data.Select(d => int.Parse(d["x"].N)).Should().BeEquivalentTo(new List<int> { total - 1 });
        }

        [Fact(DisplayName = "Paging.GetAllAsync - Test w/ Index w/ No Table Sort Key Descending")]
        public async Task TestIndexNoTableSortDescending()
        {
            var count = 25;
            var total = ((count * 4) + 1);
            var tableName = "paging-without-sk";
            await testFixture.CreateTablesAsync();

            var values = new List<int>();
            for (var x = 0; x < total; x++)
            {
                values.Add(x);
                await testFixture.AmazonDynamoDB.PutItemAsync(tableName, new Dictionary<string, AttributeValue>
                {
                    { "pk", new AttributeValue($"sort|{x.ToString("000")}") },
                    { "gsi1_pk", new AttributeValue($"index|{(x % 2).ToString("000")}") },
                    { "gsi1_sk", new AttributeValue($"sort|{x.ToString("000")}") },
                    { "x", new AttributeValue { N = x.ToString() } },
                    { "modx", new AttributeValue { N = (x % 2).ToString() } },
                });
            }

            var queryRequest = new QueryRequest
            {
                KeyConditionExpression = "gsi1_pk = :pk AND begins_with(gsi1_sk, :sk)",
                ExpressionAttributeValues = new Dictionary<string, AttributeValue>
                {
                    { ":pk", new AttributeValue("index|000") },
                    { ":sk", new AttributeValue("sort|") },
                },
                TableName = tableName,
                IndexName = "gsi1",
                Limit = count
            };

            var keyInfo = new PageKeyInfo { PartitionKeyName = "pk", IndexPartitionKeyName = "gsi1_pk", IndexSortKeyName = "gsi1_sk" };

            // null should return the last 25 records
            var result = await testFixture.AmazonDynamoDB.QueryPageAsync(queryRequest, null, SortOrder.Decending, keyInfo);

            result.Should().NotBeNull();
            result.PageInfo.Should().NotBeNull();
            result.PageInfo.ReverseKey.Should().NotBeNull();
            result.PageInfo.ForwardKey.Should().NotBeNull();
            result.Data.Count.Should().Be(count);
            result.Data.Select(d => int.Parse(d["x"].N)).Should().BeEquivalentTo(GetOrderedList(total, total - (count * 2), 2));

            // using the `ForwardKey` from the previous request should get us the next 25 records
            result = await testFixture.AmazonDynamoDB.QueryPageAsync(queryRequest, result.PageInfo.ForwardKey, SortOrder.Decending, keyInfo);

            result.Should().NotBeNull();
            result.PageInfo.Should().NotBeNull();
            result.PageInfo.ReverseKey.Should().NotBeNull();
            result.PageInfo.ForwardKey.Should().NotBeNull();
            result.Data.Count.Should().Be(count);
            result.Data.Select(d => int.Parse(d["x"].N)).Should().BeEquivalentTo(GetOrderedList(total - (count * 2), total - (count * 4), 2));

            // using the `ReverseKey` from the previous request, should give us the same records as the first request
            result = await testFixture.AmazonDynamoDB.QueryPageAsync(queryRequest, result.PageInfo.ReverseKey, SortOrder.Decending, keyInfo);

            result.Should().NotBeNull();
            result.PageInfo.Should().NotBeNull();
            result.PageInfo.ReverseKey.Should().NotBeNull();
            result.PageInfo.ForwardKey.Should().NotBeNull();
            result.Data.Count.Should().Be(count);
            result.Data.Select(d => int.Parse(d["x"].N)).Should().BeEquivalentTo(GetOrderedList(total, total - (count * 2), 2));

            // using the `ReverseKey` from the previous request, should give us no records and null keys (we are at the beginning)
            result = await testFixture.AmazonDynamoDB.QueryPageAsync(queryRequest, result.PageInfo.ReverseKey, SortOrder.Decending, keyInfo);

            result.Should().NotBeNull();
            result.PageInfo.Should().NotBeNull();
            result.PageInfo.ReverseKey.Should().BeNull();
            result.PageInfo.ForwardKey.Should().BeNull();
            result.Data.Count.Should().Be(0);

            // start again, but go to the third page
            result = await testFixture.AmazonDynamoDB.QueryPageAsync(queryRequest, null, SortOrder.Decending, keyInfo);
            result = await testFixture.AmazonDynamoDB.QueryPageAsync(queryRequest, result.PageInfo.ForwardKey, SortOrder.Decending, keyInfo);

            // this should give us the last page, and because the last record was less than the limit, we will receive a null `EndKey`, indicating we've reached the end
            result = await testFixture.AmazonDynamoDB.QueryPageAsync(queryRequest, result.PageInfo.ForwardKey, SortOrder.Decending, keyInfo);

            result.Should().NotBeNull();
            result.PageInfo.Should().NotBeNull();
            result.PageInfo.ReverseKey.Should().NotBeNull();
            result.PageInfo.ForwardKey.Should().BeNull();
            result.Data.Count.Should().Be(1);
            result.Data.Select(d => int.Parse(d["x"].N)).Should().BeEquivalentTo(new List<int> { 0 });
        }

        [Fact(DisplayName = "Paging.GetAllAsync - Test w/ Index w/ No Index Sort Key Ascending")]
        public async Task TestIndexNoIndexSortAscending()
        {
            var count = 25;
            var total = ((count * 4) + 1);
            var tableName = "paging-gsi-without-sk";
            await testFixture.CreateTablesAsync();

            var values = new List<int>();
            for (var x = 0; x < total; x++)
            {
                values.Add(x);
                await testFixture.AmazonDynamoDB.PutItemAsync(tableName, new Dictionary<string, AttributeValue>
                {
                    { "pk", new AttributeValue("partition") },
                    { "sk", new AttributeValue($"sort|{x.ToString("000")}") },
                    { "gsi1_pk", new AttributeValue($"index|{(x % 2).ToString("000")}") },
                    { "x", new AttributeValue { N = x.ToString() } },
                    { "modx", new AttributeValue { N = (x % 2).ToString() } },
                });
            }

            var queryRequest = new QueryRequest
            {
                KeyConditionExpression = "gsi1_pk = :pk",
                ExpressionAttributeValues = new Dictionary<string, AttributeValue>
                {
                    { ":pk", new AttributeValue("index|000") },
                },
                TableName = tableName,
                IndexName = "gsi1",
                Limit = count
            };

            var keyInfo = new PageKeyInfo { PartitionKeyName = "pk", SortKeyName = "sk", IndexPartitionKeyName = "gsi1_pk" };

            // null should return the first 25 records
            var result = await testFixture.AmazonDynamoDB.QueryPageAsync(queryRequest, null, SortOrder.Ascending, keyInfo);

            result.Should().NotBeNull();
            result.PageInfo.Should().NotBeNull();
            result.PageInfo.ReverseKey.Should().NotBeNull();
            result.PageInfo.ForwardKey.Should().NotBeNull();
            result.Data.Count.Should().Be(count);

            // we can't check the data because the order will be indeterminate, so we'll hold it to compare with the other pages to be sure it's not repeated
            var first = result.Data.Select(d => int.Parse(d["x"].N)).ToList();

            // using the `ForwardKey` from the previous request should get us the next 25 records
            result = await testFixture.AmazonDynamoDB.QueryPageAsync(queryRequest, result.PageInfo.ForwardKey, SortOrder.Ascending, keyInfo);

            result.Should().NotBeNull();
            result.PageInfo.Should().NotBeNull();
            result.PageInfo.ReverseKey.Should().NotBeNull();
            result.PageInfo.ForwardKey.Should().NotBeNull();
            result.Data.Count.Should().Be(count);

            // it should have different data
            result.Data.Select(d => int.Parse(d["x"].N)).Should().NotContain(first);

            // using the `ReverseKey` from the previous request, should give us the same records as the first request
            result = await testFixture.AmazonDynamoDB.QueryPageAsync(queryRequest, result.PageInfo.ReverseKey, SortOrder.Ascending, keyInfo);

            result.Should().NotBeNull();
            result.PageInfo.Should().NotBeNull();
            result.PageInfo.ReverseKey.Should().NotBeNull();
            result.PageInfo.ForwardKey.Should().NotBeNull();
            result.Data.Count.Should().Be(count);

            // it should be the same as the first time
            result.Data.Select(d => int.Parse(d["x"].N)).Should().BeEquivalentTo(first);

            // using the `ReverseKey` from the previous request, should give us no records and null keys (we are at the beginning)
            result = await testFixture.AmazonDynamoDB.QueryPageAsync(queryRequest, result.PageInfo.ReverseKey, SortOrder.Ascending, keyInfo);

            result.Should().NotBeNull();
            result.PageInfo.Should().NotBeNull();
            result.PageInfo.ReverseKey.Should().BeNull();
            result.PageInfo.ForwardKey.Should().BeNull();
            result.Data.Count.Should().Be(0);

            // start forward again, but go to the third page
            // null should return the first 25 records
            result = await testFixture.AmazonDynamoDB.QueryPageAsync(queryRequest, null, SortOrder.Ascending, keyInfo);
            result = await testFixture.AmazonDynamoDB.QueryPageAsync(queryRequest, result.PageInfo.ForwardKey, SortOrder.Ascending, keyInfo);

            // this should give us the last page, and because the last record was less than the limit, we will receive a null `EndKey`, indicating we've reached the end
            result = await testFixture.AmazonDynamoDB.QueryPageAsync(queryRequest, result.PageInfo.ForwardKey, SortOrder.Ascending, keyInfo);

            result.Should().NotBeNull();
            result.PageInfo.Should().NotBeNull();
            result.PageInfo.ReverseKey.Should().NotBeNull();
            result.PageInfo.ForwardKey.Should().BeNull();
            result.Data.Count.Should().Be(1);
        }

        [Fact(DisplayName = "Paging.GetAllAsync - Test w/ Index w/ No Index Sort Key Descending")]
        public async Task TestIndexNoIndexSortDescending()
        {
            var count = 25;
            var total = ((count * 4) + 1);
            var tableName = "paging-gsi-without-sk";
            await testFixture.CreateTablesAsync();

            var values = new List<int>();
            for (var x = 0; x < total; x++)
            {
                values.Add(x);
                await testFixture.AmazonDynamoDB.PutItemAsync(tableName, new Dictionary<string, AttributeValue>
                {
                    { "pk", new AttributeValue("partition") },
                    { "sk", new AttributeValue($"sort|{x.ToString("000")}") },
                    { "gsi1_pk", new AttributeValue($"index|{(x % 2).ToString("000")}") },
                    { "x", new AttributeValue { N = x.ToString() } },
                    { "modx", new AttributeValue { N = (x % 2).ToString() } },
                });
            }

            var queryRequest = new QueryRequest
            {
                KeyConditionExpression = "gsi1_pk = :pk",
                ExpressionAttributeValues = new Dictionary<string, AttributeValue>
                {
                    { ":pk", new AttributeValue("index|000") },
                },
                TableName = tableName,
                IndexName = "gsi1",
                Limit = count
            };

            var keyInfo = new PageKeyInfo { PartitionKeyName = "pk", SortKeyName = "sk", IndexPartitionKeyName = "gsi1_pk" };

            // null should return the first 25 records
            var result = await testFixture.AmazonDynamoDB.QueryPageAsync(queryRequest, null, SortOrder.Decending, keyInfo);

            result.Should().NotBeNull();
            result.PageInfo.Should().NotBeNull();
            result.PageInfo.ReverseKey.Should().NotBeNull();
            result.PageInfo.ForwardKey.Should().NotBeNull();
            result.Data.Count.Should().Be(count);

            // we can't check the data because the order will be indeterminate, so we'll hold it to compare with the other pages to be sure it's not repeated
            var first = result.Data.Select(d => int.Parse(d["x"].N)).ToList();

            // using the `ForwardKey` from the previous request should get us the next 25 records
            result = await testFixture.AmazonDynamoDB.QueryPageAsync(queryRequest, result.PageInfo.ForwardKey, SortOrder.Decending, keyInfo);

            result.Should().NotBeNull();
            result.PageInfo.Should().NotBeNull();
            result.PageInfo.ReverseKey.Should().NotBeNull();
            result.PageInfo.ForwardKey.Should().NotBeNull();
            result.Data.Count.Should().Be(count);

            // it should have different data
            result.Data.Select(d => int.Parse(d["x"].N)).Should().NotContain(first);

            // using the `ReverseKey` from the previous request, should give us the same records as the first request
            result = await testFixture.AmazonDynamoDB.QueryPageAsync(queryRequest, result.PageInfo.ReverseKey, SortOrder.Decending, keyInfo);

            result.Should().NotBeNull();
            result.PageInfo.Should().NotBeNull();
            result.PageInfo.ReverseKey.Should().NotBeNull();
            result.PageInfo.ForwardKey.Should().NotBeNull();
            result.Data.Count.Should().Be(count);

            // it should be the same as the first time
            result.Data.Select(d => int.Parse(d["x"].N)).Should().BeEquivalentTo(first);

            // using the `ReverseKey` from the previous request, should give us no records and null keys (we are at the beginning)
            result = await testFixture.AmazonDynamoDB.QueryPageAsync(queryRequest, result.PageInfo.ReverseKey, SortOrder.Decending, keyInfo);

            result.Should().NotBeNull();
            result.PageInfo.Should().NotBeNull();
            result.PageInfo.ReverseKey.Should().BeNull();
            result.PageInfo.ForwardKey.Should().BeNull();
            result.Data.Count.Should().Be(0);

            // start forward again, but go to the third page
            // null should return the first 25 records
            result = await testFixture.AmazonDynamoDB.QueryPageAsync(queryRequest, null, SortOrder.Decending, keyInfo);
            result = await testFixture.AmazonDynamoDB.QueryPageAsync(queryRequest, result.PageInfo.ForwardKey, SortOrder.Decending, keyInfo);

            // this should give us the last page, and because the last record was less than the limit, we will receive a null `EndKey`, indicating we've reached the end
            result = await testFixture.AmazonDynamoDB.QueryPageAsync(queryRequest, result.PageInfo.ForwardKey, SortOrder.Decending, keyInfo);

            result.Should().NotBeNull();
            result.PageInfo.Should().NotBeNull();
            result.PageInfo.ReverseKey.Should().NotBeNull();
            result.PageInfo.ForwardKey.Should().BeNull();
            result.Data.Count.Should().Be(1);
        }

        [Fact(DisplayName = "Paging.GetAllAsync - Test w/ Index w/ No Table or Index Sort Key Ascending")]
        public async Task TestIndexNoTableSortNoIndexSortAscending()
        {
            var count = 25;
            var total = ((count * 4) + 1);
            var tableName = "paging-without-sk-gsi-without-sk";
            await testFixture.CreateTablesAsync();

            var values = new List<int>();
            for (var x = 0; x < total; x++)
            {
                values.Add(x);
                await testFixture.AmazonDynamoDB.PutItemAsync(tableName, new Dictionary<string, AttributeValue>
                {
                    { "pk", new AttributeValue($"sort|{x.ToString("000")}") },
                    { "gsi1_pk", new AttributeValue($"index|{(x % 2).ToString("000")}") },
                    { "x", new AttributeValue { N = x.ToString() } },
                    { "modx", new AttributeValue { N = (x % 2).ToString() } },
                });
            }

            var queryRequest = new QueryRequest
            {
                KeyConditionExpression = "gsi1_pk = :pk",
                ExpressionAttributeValues = new Dictionary<string, AttributeValue>
                {
                    { ":pk", new AttributeValue("index|000") },
                },
                TableName = tableName,
                IndexName = "gsi1",
                Limit = count
            };

            var keyInfo = new PageKeyInfo { PartitionKeyName = "pk", IndexPartitionKeyName = "gsi1_pk" };

            // null should return the first 25 records
            var result = await testFixture.AmazonDynamoDB.QueryPageAsync(queryRequest, null, SortOrder.Ascending, keyInfo);

            result.Should().NotBeNull();
            result.PageInfo.Should().NotBeNull();
            result.PageInfo.ReverseKey.Should().NotBeNull();
            result.PageInfo.ForwardKey.Should().NotBeNull();
            result.Data.Count.Should().Be(count);

            // we can't check the data because the order will be indeterminate, so we'll hold it to compare with the other pages to be sure it's not repeated
            var first = result.Data.Select(d => int.Parse(d["x"].N)).ToList();

            // using the `ForwardKey` from the previous request should get us the next 25 records
            result = await testFixture.AmazonDynamoDB.QueryPageAsync(queryRequest, result.PageInfo.ForwardKey, SortOrder.Ascending, keyInfo);

            result.Should().NotBeNull();
            result.PageInfo.Should().NotBeNull();
            result.PageInfo.ReverseKey.Should().NotBeNull();
            result.PageInfo.ForwardKey.Should().NotBeNull();
            result.Data.Count.Should().Be(count);

            // it should have different data
            result.Data.Select(d => int.Parse(d["x"].N)).Should().NotContain(first);

            // using the `ReverseKey` from the previous request, should give us the same records as the first request
            result = await testFixture.AmazonDynamoDB.QueryPageAsync(queryRequest, result.PageInfo.ReverseKey, SortOrder.Ascending, keyInfo);

            result.Should().NotBeNull();
            result.PageInfo.Should().NotBeNull();
            result.PageInfo.ReverseKey.Should().NotBeNull();
            result.PageInfo.ForwardKey.Should().NotBeNull();
            result.Data.Count.Should().Be(count);

            // it should be the same as the first time
            result.Data.Select(d => int.Parse(d["x"].N)).Should().BeEquivalentTo(first);

            // using the `ReverseKey` from the previous request, should give us no records and null keys (we are at the beginning)
            result = await testFixture.AmazonDynamoDB.QueryPageAsync(queryRequest, result.PageInfo.ReverseKey, SortOrder.Ascending, keyInfo);

            result.Should().NotBeNull();
            result.PageInfo.Should().NotBeNull();
            result.PageInfo.ReverseKey.Should().BeNull();
            result.PageInfo.ForwardKey.Should().BeNull();
            result.Data.Count.Should().Be(0);

            // start forward again, but go to the third page
            // null should return the first 25 records
            result = await testFixture.AmazonDynamoDB.QueryPageAsync(queryRequest, null, SortOrder.Ascending, keyInfo);
            result = await testFixture.AmazonDynamoDB.QueryPageAsync(queryRequest, result.PageInfo.ForwardKey, SortOrder.Ascending, keyInfo);

            // this should give us the last page, and because the last record was less than the limit, we will receive a null `EndKey`, indicating we've reached the end
            result = await testFixture.AmazonDynamoDB.QueryPageAsync(queryRequest, result.PageInfo.ForwardKey, SortOrder.Ascending, keyInfo);

            result.Should().NotBeNull();
            result.PageInfo.Should().NotBeNull();
            result.PageInfo.ReverseKey.Should().NotBeNull();
            result.PageInfo.ForwardKey.Should().BeNull();
            result.Data.Count.Should().Be(1);
        }

        [Fact(DisplayName = "Paging.GetAllAsync - Test w/ Index w/ No Table or Index Sort Key Descending")]
        public async Task TestIndexNoTableSortNoIndexSortDescending()
        {
            var count = 25;
            var total = ((count * 4) + 1);
            var tableName = "paging-without-sk-gsi-without-sk";
            await testFixture.CreateTablesAsync();

            var values = new List<int>();
            for (var x = 0; x < total; x++)
            {
                values.Add(x);
                await testFixture.AmazonDynamoDB.PutItemAsync(tableName, new Dictionary<string, AttributeValue>
                {
                    { "pk", new AttributeValue($"sort|{x.ToString("000")}") },
                    { "gsi1_pk", new AttributeValue($"index|{(x % 2).ToString("000")}") },
                    { "x", new AttributeValue { N = x.ToString() } },
                    { "modx", new AttributeValue { N = (x % 2).ToString() } },
                });
            }

            var queryRequest = new QueryRequest
            {
                KeyConditionExpression = "gsi1_pk = :pk",
                ExpressionAttributeValues = new Dictionary<string, AttributeValue>
                {
                    { ":pk", new AttributeValue("index|000") },
                },
                TableName = tableName,
                IndexName = "gsi1",
                Limit = count
            };

            var keyInfo = new PageKeyInfo { PartitionKeyName = "pk", IndexPartitionKeyName = "gsi1_pk" };

            // null should return the first 25 records
            var result = await testFixture.AmazonDynamoDB.QueryPageAsync(queryRequest, null, SortOrder.Decending, keyInfo);

            result.Should().NotBeNull();
            result.PageInfo.Should().NotBeNull();
            result.PageInfo.ReverseKey.Should().NotBeNull();
            result.PageInfo.ForwardKey.Should().NotBeNull();
            result.Data.Count.Should().Be(count);

            // we can't check the data because the order will be indeterminate, so we'll hold it to compare with the other pages to be sure it's not repeated
            var first = result.Data.Select(d => int.Parse(d["x"].N)).ToList();

            // using the `ForwardKey` from the previous request should get us the next 25 records
            result = await testFixture.AmazonDynamoDB.QueryPageAsync(queryRequest, result.PageInfo.ForwardKey, SortOrder.Decending, keyInfo);

            result.Should().NotBeNull();
            result.PageInfo.Should().NotBeNull();
            result.PageInfo.ReverseKey.Should().NotBeNull();
            result.PageInfo.ForwardKey.Should().NotBeNull();
            result.Data.Count.Should().Be(count);

            // it should have different data
            result.Data.Select(d => int.Parse(d["x"].N)).Should().NotContain(first);

            // using the `ReverseKey` from the previous request, should give us the same records as the first request
            result = await testFixture.AmazonDynamoDB.QueryPageAsync(queryRequest, result.PageInfo.ReverseKey, SortOrder.Decending, keyInfo);

            result.Should().NotBeNull();
            result.PageInfo.Should().NotBeNull();
            result.PageInfo.ReverseKey.Should().NotBeNull();
            result.PageInfo.ForwardKey.Should().NotBeNull();
            result.Data.Count.Should().Be(count);

            // it should be the same as the first time
            result.Data.Select(d => int.Parse(d["x"].N)).Should().BeEquivalentTo(first);

            // using the `ReverseKey` from the previous request, should give us no records and null keys (we are at the beginning)
            result = await testFixture.AmazonDynamoDB.QueryPageAsync(queryRequest, result.PageInfo.ReverseKey, SortOrder.Decending, keyInfo);

            result.Should().NotBeNull();
            result.PageInfo.Should().NotBeNull();
            result.PageInfo.ReverseKey.Should().BeNull();
            result.PageInfo.ForwardKey.Should().BeNull();
            result.Data.Count.Should().Be(0);

            // start forward again, but go to the third page
            // null should return the first 25 records
            result = await testFixture.AmazonDynamoDB.QueryPageAsync(queryRequest, null, SortOrder.Decending, keyInfo);
            result = await testFixture.AmazonDynamoDB.QueryPageAsync(queryRequest, result.PageInfo.ForwardKey, SortOrder.Decending, keyInfo);

            // this should give us the last page, and because the last record was less than the limit, we will receive a null `EndKey`, indicating we've reached the end
            result = await testFixture.AmazonDynamoDB.QueryPageAsync(queryRequest, result.PageInfo.ForwardKey, SortOrder.Decending, keyInfo);

            result.Should().NotBeNull();
            result.PageInfo.Should().NotBeNull();
            result.PageInfo.ReverseKey.Should().NotBeNull();
            result.PageInfo.ForwardKey.Should().BeNull();
            result.Data.Count.Should().Be(1);
        }

        private List<int> GetOrderedList(int start, int end, int step = 1)
        {
            var list = new List<int>();
            if (start <= end)
            {
                for (var i = start; i < end; i += step)
                {
                    list.Add(i);
                }
            }
            else
            {
                for (var i = start - 1; i >= end; i -= step)
                {
                    list.Add(i);
                }
            }

            return list;
        }
    }
}
