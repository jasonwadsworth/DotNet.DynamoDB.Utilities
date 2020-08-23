using Xunit;

namespace DynamoDB.Utilities.Tests
{
    [CollectionDefinition("TestCollection")]
    public class TestCollection : ICollectionFixture<TestFixture>
    {
    }
}
