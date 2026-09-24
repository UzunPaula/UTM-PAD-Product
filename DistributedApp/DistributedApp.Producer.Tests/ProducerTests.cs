using DistributedApp.Contracts;
using DistributedApp.Producer;

namespace DistributedApp.Producer.Tests;

public class ProducerTests
{
    [Theory]
    [InlineData("", "Keyboard", 1)]
    [InlineData("ORD-1", "", 1)]
    [InlineData("ORD-1", "Keyboard", 0)]
    [InlineData("ORD-1", "Keyboard", 1001)]
    public void ValidateOrder_RejectsInvalidInput(
        string orderId,
        string product,
        int quantity)
    {
        OrderPayload order = new()
        {
            OrderId = orderId,
            Product = product,
            Quantity = quantity
        };

        Assert.NotNull(Producer.ValidateOrder(order));
    }

    [Fact]
    public void ValidateOrder_AcceptsValidInput()
    {
        OrderPayload order = new()
        {
            OrderId = "ORD-1",
            Product = "Keyboard",
            Quantity = 2
        };

        Assert.Null(Producer.ValidateOrder(order));
    }
}
