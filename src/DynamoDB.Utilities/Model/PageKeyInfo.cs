namespace Wadsworth.DynamoDB.Utilities.Model
{
    /// <summary>
    /// Paging key info.
    /// </summary>
    public class PageKeyInfo
    {
        /// <summary>
        /// Gets or sets the name of the table's partition key (required).
        /// </summary>
        public string PartitionKeyName { get; set; }

        /// <summary>
        /// Gets or sets the name of the table's sort key (required if the table has a sort key).
        /// </summary>
        public string SortKeyName { get; set; }

        /// <summary>
        /// Gets or sets the name of the index's partition key (required if querying an index).
        /// </summary>
        public string IndexPartitionKeyName { get; set; }

        /// <summary>
        /// Gets or sets the name of the index's sort key (required if querying an index with a sort key).
        /// </summary>
        public string IndexSortKeyName { get; set; }
    }

}
