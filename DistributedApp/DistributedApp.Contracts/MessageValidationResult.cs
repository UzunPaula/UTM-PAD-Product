namespace DistributedApp.Contracts;

public sealed class MessageValidationResult
{
    internal MessageValidationResult(IReadOnlyList<string> errors)
    {
        Errors = errors;
    }

    public bool IsValid => Errors.Count == 0;

    public IReadOnlyList<string> Errors { get; }
}
