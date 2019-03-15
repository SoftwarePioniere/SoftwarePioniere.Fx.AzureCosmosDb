namespace SoftwarePioniere.ReadModel.Services.AzureCosmosDb
{
    public class AzureCosmosDbOptions : EntityStoreOptionsBase
    {
        public string CollectionId { get; set; }

        public string AuthKey { get; set; } = "C2y6yDjf5/R+ob0N8A7Cgv30VRDJIWEHLM+4QDU5DE2nQ9nDuVTqobD4b8mGGyPMbIZnqyMsEcaGQy67XIw/Jw==";

        public string DatabaseId { get; set; } = "sopidev";

        public string EndpointUrl { get; set; } = "https://localhost:8081";

        public bool ScaleOfferThroughput { get; set; } = false;

        public int OfferThroughput { get; set; } = 400;

        public override string ToString()
        {
            return $"EndpointUrl: {EndpointUrl} // DatabaseId: {DatabaseId} // CollectionId: {CollectionId} ";
        }

    }
}
