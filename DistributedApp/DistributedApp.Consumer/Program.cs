using DistributedApp.Consumer;

var consumer = new ConsumerClient("127.0.0.1", 5000);

try
{
    await consumer.ConnectAsync();

    await consumer.SubscribeAsync("orders");

    Console.WriteLine("Consumer is running.");
    Console.WriteLine("Press Enter to stop.");

    Console.ReadLine();
}
catch (Exception ex)
{
    Console.WriteLine($"Consumer error: {ex.Message}");
}