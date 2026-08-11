using System;
using System.Collections.Concurrent;
using System.Threading;

namespace Ma7MQ.Core.Broker
{
    public class TokenBucket
    {
        private readonly object _lock = new();
        private readonly double _rate; // tokens per second
        private readonly double _capacity;
        private double _tokens;
        private DateTime _lastRefilled;

        public TokenBucket(double rate, double capacity)
        {
            _rate = rate;
            _capacity = capacity;
            _tokens = capacity;
            _lastRefilled = DateTime.UtcNow;
        }

        public bool Allow()
        {
            lock (_lock)
            {
                var now = DateTime.UtcNow;
                var elapsed = (now - _lastRefilled).TotalSeconds;
                _lastRefilled = now;

                _tokens = _tokens + elapsed * _rate;
                if (_tokens > _capacity)
                {
                    _tokens = _capacity;
                }

                if (_tokens >= 1.0)
                {
                    _tokens -= 1.0;
                    return true;
                }
                return false;
            }
        }
    }

    public class RateLimiter
    {
        private readonly ConcurrentDictionary<string, TokenBucket> _clients = new();
        private readonly double _rate;
        private readonly double _burst;

        public RateLimiter(double rate, double burst)
        {
            _rate = rate;
            _burst = burst;
        }

        public bool Allow(string clientID)
        {
            var bucket = _clients.GetOrAdd(clientID, _ => new TokenBucket(_rate, _burst));
            return bucket.Allow();
        }
    }
}
