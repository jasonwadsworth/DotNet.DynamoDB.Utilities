namespace Wadsworth.DynamoDB.Utilities.Model
{
    /// <summary>
    /// Paging info.
    /// </summary>
    public class PageInfo
    {
        /// <summary>
        /// Gets or sets the key used to page in reverse.
        /// </summary>
        public string ReverseKey { get; set; }

        /// <summary>
        /// Gets or sets the key used to page forward.
        /// </summary>
        public string ForwardKey { get; set; }
    }
}
