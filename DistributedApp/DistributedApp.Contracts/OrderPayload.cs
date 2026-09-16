namespace DistributedApp.Contracts;

public class OrderPayload
{
    public string OrderId { get; set; } = string.Empty;

    public string Product { get; set; } = string.Empty;

    public int Quantity { get; set; }
}