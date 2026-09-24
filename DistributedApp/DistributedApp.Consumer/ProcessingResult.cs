namespace DistributedApp.Consumer;

public enum ProcessingStatus
{
    Processed,
    Duplicate,
    Failed
}

public sealed record ProcessingResult(
    ProcessingStatus Status,
    string? Error)
{
    public static ProcessingResult Processed()
    {
        return new ProcessingResult(ProcessingStatus.Processed, null);
    }

    public static ProcessingResult Duplicate()
    {
        return new ProcessingResult(ProcessingStatus.Duplicate, null);
    }

    public static ProcessingResult Failed(string error)
    {
        return new ProcessingResult(ProcessingStatus.Failed, error);
    }
}
