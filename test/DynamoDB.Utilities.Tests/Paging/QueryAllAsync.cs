using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Amazon.DynamoDBv2.Model;
using FluentAssertions;
using Wadsworth.DynamoDB.Utilities;
using Xunit;

namespace DynamoDB.Utilities.Tests.Paging
{
    [Collection("TestCollection")]
    public class GetAllAsync
    {
        private readonly TestFixture testFixture;

        public GetAllAsync(TestFixture testFixture)
        {
            this.testFixture = testFixture;
        }

        [Fact(DisplayName = "Paging.GetAllAsync - Test w/o Index")]
        public async Task TestNoIndex()
        {
            var tableName = "paging-with-sk";
            await testFixture.CreateTablesAsync();

            var values = new List<int>();
            for (var x = 0; x < 100; x++)
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
                TableName = tableName
            };

            var all = await testFixture.AmazonDynamoDB.QueryAllAsync(queryRequest);

            all.Count.Should().Be(100);
            all.Select(a => int.Parse(a["x"].N)).Should().BeEquivalentTo(values);
        }

        [Fact(DisplayName = "Paging.GetAllAsync - Test w/ Index")]
        public async Task TestWithIndex()
        {
            var tableName = "paging-with-sk";
            await testFixture.CreateTablesAsync();

            var values = new List<int>();
            for (var x = 0; x < 100; x++)
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
                IndexName = "gsi1"
            };

            var all = await testFixture.AmazonDynamoDB.QueryAllAsync(queryRequest);

            all.Count.Should().Be(50);
            all.Select(a => int.Parse(a["x"].N)).Should().BeEquivalentTo(values.Where(v => v % 2 == 0));
        }
    }
}
