using System;
using System.Collections.Generic;
using System.Text;
using System.Text.Json;
using System.Threading.Tasks;
using Amazon.DynamoDBv2;
using Amazon.DynamoDBv2.Model;
using Wadsworth.DynamoDB.Utilities.Model;

namespace Wadsworth.DynamoDB.Utilities
{
    public static class Paging
    {
        /// <summary>
        /// Queries the data using the queryRequest and returns a complete list of data matching the query.
        /// </summary>
        /// <remarks>
        /// NOTE: the ExclusiveStartKey and Limit values of the queryRequest will be overridden by this method.
        ///
        /// CAUTION: this method will return all values without concern for memory
        /// </remarks>
        /// <param name="dynamoDB">The DynamoDB client.</param>
        /// <param name="queryRequest">The query request.</param>
        /// <returns>A complete list of matching records.</returns>
        public static async Task<List<Dictionary<string, AttributeValue>>> QueryAllAsync(this IAmazonDynamoDB dynamoDB, QueryRequest queryRequest)
        {
            var items = new List<Dictionary<string, AttributeValue>>();

            Dictionary<string, AttributeValue> exclusiveStartKey = null;

            do
            {
                var batch = await queryBatchAsync();
                items.AddRange(batch);
            } while (exclusiveStartKey != null && exclusiveStartKey.Count != 0);

            return items;

            async Task<List<Dictionary<string, AttributeValue>>> queryBatchAsync()
            {
                queryRequest.ExclusiveStartKey = exclusiveStartKey;
                queryRequest.Limit = int.MaxValue;

                var result = await dynamoDB.QueryAsync(queryRequest);
                exclusiveStartKey = result.LastEvaluatedKey;
                return result.Items;
            }
        }

        /// <summary>
        /// Queries the data using the queryRequest and paging info.
        ///
        /// This method only supports tables with keys that are strings.
        /// </summary>
        /// <remarks>
        /// NOTE: the ExclusiveStartKey and ScanIndexForward values of the queryRequest will be overridden by this method.<br/>
        /// <br/>
        /// queryResult properties should be the same for all requests for the page, with the exception of ExclusiveStartKey and ScanIndexForward. Using a different query will yield unpredictable results.<br/>
        /// <br/>
        /// pageKey should be null on the first request. All subsequent requests should use the start or end key from the page results.<br/>
        ///  - Using the start key will give you the records from the previous page, in order based on the sort order
        ///     (i.e. if you request items in ascending order you will get the page in ascending order, where the last record should be the record just before the first record of the current page)<br/>
        ///  - Using the end key will give you the records from the next page, in order based on the sort order
        ///     (i.e. if you request items in ascending order you will get the page in ascending order, where the first record should be the record just after the last record of the current page)<br/>
        /// <br/>
        /// sortOrder will determine the direction of the data (i.e. A to Z vs. Z to A).<br/>
        ///   NOTE: the sort order will not be as expected when there isn't a sort key in the query. This is consistent with how DynamoDB queries work when no sort key is present.<br/>
        /// <br/>
        /// keyInfo should include relevant key info about the table and, if appropriate, index key info. The partition key is always required. Sort key is required if the table has a sort key.<br/>
        ///   Index partition and sort keys are required if using an index (index sort key is only required if the index has a sort key).<br/>
        /// </remarks>
        /// <param name="dynamoDB">The DynamoDB client.</param>
        /// <param name="queryRequest">The query request.</param>
        /// <param name="pageKey">The key for determining what records to return next.</param>
        /// <param name="sortOrder">The sort order.</param>
        /// <param name="keyInfo">The table's key info.</param>
        /// <returns>A a page of matching records.</returns>
        public static async Task<PageResult> QueryPageAsync(this IAmazonDynamoDB dynamoDB, QueryRequest queryRequest, string pageKey, SortOrder sortOrder, PageKeyInfo keyInfo)
        {
            Key key = null;
            if (pageKey != null)
            {
                // get the key from the pageKey
                key = JsonSerializer.Deserialize<Key>(Encoding.UTF8.GetString(Convert.FromBase64String(pageKey)), new JsonSerializerOptions { PropertyNameCaseInsensitive = true });
            }

            // use the key to determine if the query should go forward or backward
            var forward = key == null ? true : key.Forward;

            // the "exclusiveStartKey" is used in the DynamoDB query. We'll created it here from the key
            var exclusiveStartKey = key == null ? null : new Dictionary<string, AttributeValue>
            {
                { keyInfo.PartitionKeyName, new AttributeValue(key.Pk) },
            };

            if (exclusiveStartKey != null)
            {
                if (!string.IsNullOrEmpty(keyInfo.SortKeyName))
                    exclusiveStartKey.Add(keyInfo.SortKeyName, new AttributeValue(key.Sk));
                if (!string.IsNullOrEmpty(keyInfo.IndexPartitionKeyName))
                    exclusiveStartKey.Add(keyInfo.IndexPartitionKeyName, new AttributeValue(key.GsiPk));
                if (!string.IsNullOrEmpty(keyInfo.IndexSortKeyName))
                    exclusiveStartKey.Add(keyInfo.IndexSortKeyName, new AttributeValue(key.GsiSk));
            }

            queryRequest.ExclusiveStartKey = exclusiveStartKey;
            queryRequest.ScanIndexForward = sortOrder == SortOrder.Decending ? !forward : forward;

            var result = await dynamoDB.QueryAsync(queryRequest);

            var items = result.Items;

            // start key is the first record of the result
            var startKey = items.Count == 0 ? null : new Key
            {
                Forward = false,
                Pk = items[0][keyInfo.PartitionKeyName].S,
                Sk = !string.IsNullOrEmpty(keyInfo.SortKeyName) && items[0].ContainsKey(keyInfo.SortKeyName) ? items[0][keyInfo.SortKeyName].S : null,
                GsiPk = !string.IsNullOrEmpty(keyInfo.IndexPartitionKeyName) && items[0].ContainsKey(keyInfo.IndexPartitionKeyName) ? items[0][keyInfo.IndexPartitionKeyName].S : null,
                GsiSk = !string.IsNullOrEmpty(keyInfo.IndexSortKeyName) && items[0].ContainsKey(keyInfo.IndexSortKeyName) ? items[0][keyInfo.IndexSortKeyName].S : null,
            };

            // if we are going through the data backward we want to reverse the data
            if (!forward)
                items.Reverse();

            // use the LastEvaluatedKey to create the end key (this may be the last record of the result, but may not be)
            var endKey = result.LastEvaluatedKey == null || result.LastEvaluatedKey.Count == 0 ? null : new Key
            {
                Forward = true,
                Pk = result.LastEvaluatedKey[keyInfo.PartitionKeyName].S,
                Sk = !string.IsNullOrEmpty(keyInfo.SortKeyName) && result.LastEvaluatedKey.ContainsKey(keyInfo.SortKeyName) ? result.LastEvaluatedKey[keyInfo.SortKeyName].S : null,
                GsiPk = !string.IsNullOrEmpty(keyInfo.IndexPartitionKeyName) && result.LastEvaluatedKey.ContainsKey(keyInfo.IndexPartitionKeyName) ? result.LastEvaluatedKey[keyInfo.IndexPartitionKeyName].S : null,
                GsiSk = !string.IsNullOrEmpty(keyInfo.IndexSortKeyName) && result.LastEvaluatedKey.ContainsKey(keyInfo.IndexSortKeyName) ? result.LastEvaluatedKey[keyInfo.IndexSortKeyName].S : null,
            };

            // if going backward then reverse the keys
            if (!forward)
            {
                var holder = startKey;
                startKey = endKey;
                if (startKey != null)
                    startKey.Forward = false;
                endKey = holder;
                if (endKey != null)
                    endKey.Forward = true;
            }

            // we will always return something here, even if there are no results.
            // This is because techincally we could have more to get (the end cursor might not be null) and it makes it a little easier for the client if there is always an object
            return new PageResult
            {
                Data = items,
                PageInfo = new PageInfo
                {
                    ReverseKey = startKey == null ? null : Convert.ToBase64String(Encoding.UTF8.GetBytes(JsonSerializer.Serialize(startKey, new JsonSerializerOptions { PropertyNamingPolicy = JsonNamingPolicy.CamelCase, IgnoreNullValues = true }))),
                    ForwardKey = endKey == null ? null : Convert.ToBase64String(Encoding.UTF8.GetBytes(JsonSerializer.Serialize(endKey, new JsonSerializerOptions { PropertyNamingPolicy = JsonNamingPolicy.CamelCase, IgnoreNullValues = true })))
                }
            };
        }

        /// <summary>
        /// Class for serializing keys for paging.
        /// </summary>
        private class Key
        {
            /// <summary>
            /// Gets or sets a flag indicating whether the key is for going forward or backward.
            /// </summary>
            public bool Forward { get; set; }

            /// <summary>
            /// Gets or sets the partition key value.
            /// </summary>
            public string Pk { get; set; }

            /// <summary>
            /// Gets or sets the sort key value.
            /// </summary>
            public string Sk { get; set; }

            /// <summary>
            /// Gets or sets the GSI partition key value.
            /// </summary>
            public string GsiPk { get; set; }

            /// <summary>
            /// Gets or sets the GSI sort key value.
            /// </summary>
            public string GsiSk { get; set; }
        }
    }
}
