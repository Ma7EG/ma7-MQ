using System;
using System.Threading.Tasks;

namespace Ma7MQ.Core.Broker
{
    public enum CircuitState
    {
        Closed,
        Open,
        HalfOpen
    }

    public class CircuitBreakerOpenException : Exception
    {
        public CircuitBreakerOpenException() : base("Circuit breaker is open; request rejected.") { }
    }

    public class CircuitBreaker
    {
        private readonly object _lock = new();
        private CircuitState _state = CircuitState.Closed;
        private int _failures = 0;
        private readonly int _threshold;
        private readonly TimeSpan _timeout;
        private DateTime _lastFailTime;

        public CircuitBreaker(int threshold, TimeSpan timeout)
        {
            _threshold = threshold;
            _timeout = timeout;
        }

        public async Task ExecuteAsync(Func<Task> action)
        {
            if (!AllowRequest())
            {
                throw new CircuitBreakerOpenException();
            }

            try
            {
                await action();
                RecordSuccess();
            }
            catch
            {
                RecordFailure();
                throw;
            }
        }

        private bool AllowRequest()
        {
            lock (_lock)
            {
                if (_state == CircuitState.Open)
                {
                    if (DateTime.UtcNow - _lastFailTime > _timeout)
                    {
                        _state = CircuitState.HalfOpen;
                        return true;
                    }
                    return false;
                }
                return true;
            }
        }

        private void RecordFailure()
        {
            lock (_lock)
            {
                _failures++;
                _lastFailTime = DateTime.UtcNow;

                if (_state == CircuitState.HalfOpen || _failures >= _threshold)
                {
                    _state = CircuitState.Open;
                }
            }
        }

        private void RecordSuccess()
        {
            lock (_lock)
            {
                _failures = 0;
                _state = CircuitState.Closed;
            }
        }

        public CircuitState State
        {
            get
            {
                lock (_lock)
                {
                    return _state;
                }
            }
        }
    }
}
// Concurrency optimized
