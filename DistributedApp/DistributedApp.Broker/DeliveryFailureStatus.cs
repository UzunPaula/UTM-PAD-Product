namespace DistributedApp.Broker;

public enum DeliveryFailureStatus
{
    NotFound,
    Retrying,
    DeadLettered
}
