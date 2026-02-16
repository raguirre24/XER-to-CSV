using System;

namespace XerToCsvConverter;

// Helper class for thread-safe progress counting
internal class ProgressCounter(int total, IProgress<(int percent, string message)>? progress)
{
    private readonly int _total = total;
    private readonly IProgress<(int percent, string message)>? _progress = progress;
    private int _count;
    private readonly object _lock = new();

    public void Increment(string message)
    {
        lock (_lock)
        {
            _count++;
            Report(message);
        }
    }

    public void UpdateStatus(string message)
    {
        lock (_lock)
        {
            Report(message);
        }
    }

    private void Report(string message)
    {
        if (_progress == null || _total <= 0) return;

        int percent = (int)((double)_count / _total * 100);
        percent = Math.Min(100, percent);
        int adjustedPercent = 80 + (int)(percent * 0.2);
        _progress.Report((adjustedPercent, $"{message} ({_count}/{_total})"));
    }
}
