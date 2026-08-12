using System;

namespace Ma7MQ.Core.Broker
{
    public class RetryPolicy
    {
        public int MaxAttempts { get; set; }
        public TimeSpan BaseDelay { get; set; }
        public TimeSpan MaxDelay { get; set; }
        private readonly Random _random = new();

        public RetryPolicy(int maxAttempts, TimeSpan baseDelay, TimeSpan maxDelay)
        {
            MaxAttempts = maxAttempts;
            BaseDelay = baseDelay;
            MaxDelay = maxDelay;
        }

        public TimeSpan CalculateDelay(int attempt)
        {
            double temp = BaseDelay.TotalMilliseconds * Math.Pow(2, attempt);
            TimeSpan delay = TimeSpan.FromMilliseconds(temp);

            if (delay > MaxDelay)
            {
                delay = MaxDelay;
            }

            // Jitter: +/- 10%
            int jitterRange = (int)(delay.TotalMilliseconds / 10);
            if (jitterRange > 0)
            {
                int jitter = _random.Next(jitterRange);
                delay = delay.Add(TimeSpan.FromMilliseconds(jitter));
            }

            return delay;
        }
    }
}
