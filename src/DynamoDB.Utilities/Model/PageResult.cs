using System.Collections.Generic;
using Amazon.DynamoDBv2.Model;

namespace Wadsworth.DynamoDB.Utilities.Model
{
    /// <summary>
    /// Paging results.
    /// </summary>
    public class PageResult
    {
        /// <summary>
        /// Gets or sets the data of the page.
        /// </summary>
        public List<Dictionary<string, AttributeValue>> Data { get; set; }

        /// <summary>
        /// Gets or sets the page info.
        /// </summary>
        public PageInfo PageInfo { get; set; }
    }
}
