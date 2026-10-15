namespace DistributedApp.Consumer;

public static class ConsumerStateFiles
{
    public static bool IsValidConsumerId(string? consumerId) =>
        !string.IsNullOrEmpty(consumerId)
        && consumerId.Length <= 64
        && IsAsciiLetterOrDigit(consumerId[0])
        && consumerId.All(character => IsAsciiLetterOrDigit(character) || character is '.' or '_' or '-');

    public static string ResolvePath(string consumerId, string? configuredPath = null)
    {
        if (!IsValidConsumerId(consumerId))
            throw new ArgumentException("A valid consumerId is required.", nameof(consumerId));

        if (!string.IsNullOrWhiteSpace(configuredPath))
        {
            if (!Path.IsPathFullyQualified(configuredPath))
                throw new ArgumentException("CONSUMER_STATE_FILE must be an absolute path.", nameof(configuredPath));
            return Path.GetFullPath(configuredPath);
        }

        string localData = Environment.GetFolderPath(Environment.SpecialFolder.LocalApplicationData);
        if (string.IsNullOrWhiteSpace(localData))
            throw new IOException("The local application data folder could not be found.");

        return Path.Combine(localData, "DistributedApp", "Consumer", $"consumer-{consumerId}.json");
    }

    public static FileStream AcquireExclusiveLock(string stateFile)
    {
        string lockFile = Path.GetFullPath(stateFile) + ".lock";
        Directory.CreateDirectory(Path.GetDirectoryName(lockFile)!);
        try
        {
            // Keep the handle open for the entire process lifetime; the OS releases it on exit.
            return new FileStream(lockFile, FileMode.OpenOrCreate, FileAccess.ReadWrite, FileShare.None);
        }
        catch (IOException exception)
        {
            throw new IOException($"State file '{stateFile}' is already used by another Consumer or cannot be locked.",
                exception);
        }
    }

    private static bool IsAsciiLetterOrDigit(char character) =>
        character is >= 'a' and <= 'z' or >= 'A' and <= 'Z' or >= '0' and <= '9';
}
