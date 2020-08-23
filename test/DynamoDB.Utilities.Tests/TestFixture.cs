using System.Collections.Generic;
using Amazon.DynamoDBv2;
using Amazon.DynamoDBv2.Model;

namespace DynamoDB.Utilities.Tests
{
    public class TestFixture : Wadsworth.DynamoDB.TestUtilities.DynamoDBLocalFixture
    {
        protected override List<CreateTableRequest> GetCreateTableRequests()
        {
            return new List<CreateTableRequest>
            {
                new CreateTableRequest
                {
                    AttributeDefinitions = new System.Collections.Generic.List<AttributeDefinition>
                    {
                        new AttributeDefinition("pk", ScalarAttributeType.S),
                        new AttributeDefinition("sk", ScalarAttributeType.S),
                        new AttributeDefinition("gsi1_pk", ScalarAttributeType.S),
                        new AttributeDefinition("gsi1_sk", ScalarAttributeType.S),
                    },
                    BillingMode = BillingMode.PAY_PER_REQUEST,
                    KeySchema = new System.Collections.Generic.List<KeySchemaElement>
                    {
                        new KeySchemaElement("pk", KeyType.HASH),
                        new KeySchemaElement("sk", KeyType.RANGE)
                    },
                    GlobalSecondaryIndexes = new System.Collections.Generic.List<GlobalSecondaryIndex>
                    {
                        new GlobalSecondaryIndex
                        {
                            IndexName = "gsi1",
                            KeySchema = new System.Collections.Generic.List<KeySchemaElement>
                            {
                                new KeySchemaElement("gsi1_pk", KeyType.HASH),
                                new KeySchemaElement("gsi1_sk", KeyType.RANGE),
                            },
                            Projection = new Projection
                            {
                                ProjectionType = ProjectionType.ALL
                            }
                        },
                    },
                    TableName = "paging-with-sk"
                },
                new CreateTableRequest
                {
                    AttributeDefinitions = new System.Collections.Generic.List<AttributeDefinition>
                    {
                        new AttributeDefinition("pk", ScalarAttributeType.S),
                        new AttributeDefinition("gsi1_pk", ScalarAttributeType.S),
                        new AttributeDefinition("gsi1_sk", ScalarAttributeType.S),
                    },
                    BillingMode = BillingMode.PAY_PER_REQUEST,
                    KeySchema = new System.Collections.Generic.List<KeySchemaElement>
                    {
                        new KeySchemaElement("pk", KeyType.HASH),
                    },
                    GlobalSecondaryIndexes = new System.Collections.Generic.List<GlobalSecondaryIndex>
                    {
                        new GlobalSecondaryIndex
                        {
                            IndexName = "gsi1",
                            KeySchema = new System.Collections.Generic.List<KeySchemaElement>
                            {
                                new KeySchemaElement("gsi1_pk", KeyType.HASH),
                                new KeySchemaElement("gsi1_sk", KeyType.RANGE),
                            },
                            Projection = new Projection
                            {
                                ProjectionType = ProjectionType.ALL
                            }
                        },
                    },
                    TableName = "paging-without-sk"
                },
                new CreateTableRequest
                {
                    AttributeDefinitions = new System.Collections.Generic.List<AttributeDefinition>
                    {
                        new AttributeDefinition("pk", ScalarAttributeType.S),
                        new AttributeDefinition("sk", ScalarAttributeType.S),
                        new AttributeDefinition("gsi1_pk", ScalarAttributeType.S),
                    },
                    BillingMode = BillingMode.PAY_PER_REQUEST,
                    KeySchema = new System.Collections.Generic.List<KeySchemaElement>
                    {
                        new KeySchemaElement("pk", KeyType.HASH),
                        new KeySchemaElement("sk", KeyType.RANGE)
                    },
                    GlobalSecondaryIndexes = new System.Collections.Generic.List<GlobalSecondaryIndex>
                    {
                        new GlobalSecondaryIndex
                        {
                            IndexName = "gsi1",
                            KeySchema = new System.Collections.Generic.List<KeySchemaElement>
                            {
                                new KeySchemaElement("gsi1_pk", KeyType.HASH),
                            },
                            Projection = new Projection
                            {
                                ProjectionType = ProjectionType.ALL
                            }
                        },
                    },
                    TableName = "paging-gsi-without-sk"
                },
                new CreateTableRequest
                {
                    AttributeDefinitions = new System.Collections.Generic.List<AttributeDefinition>
                    {
                        new AttributeDefinition("pk", ScalarAttributeType.S),
                        new AttributeDefinition("gsi1_pk", ScalarAttributeType.S),
                    },
                    BillingMode = BillingMode.PAY_PER_REQUEST,
                    KeySchema = new System.Collections.Generic.List<KeySchemaElement>
                    {
                        new KeySchemaElement("pk", KeyType.HASH),
                    },
                    GlobalSecondaryIndexes = new System.Collections.Generic.List<GlobalSecondaryIndex>
                    {
                        new GlobalSecondaryIndex
                        {
                            IndexName = "gsi1",
                            KeySchema = new System.Collections.Generic.List<KeySchemaElement>
                            {
                                new KeySchemaElement("gsi1_pk", KeyType.HASH),
                            },
                            Projection = new Projection
                            {
                                ProjectionType = ProjectionType.ALL
                            }
                        },
                    },
                    TableName = "paging-without-sk-gsi-without-sk"
                },
            };
        }
    }
}
