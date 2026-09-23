namespace DistributedApp.Contracts;

public static class MessageSchema
{
    public const string CurrentVersion = "1.0";

    public static bool IsSupported(string? version)
    {
        return string.Equals(
            version,
            CurrentVersion,
            StringComparison.Ordinal);
    }
}
