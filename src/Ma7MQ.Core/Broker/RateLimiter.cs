using System;
using System.Threading.Tasks;
using StackExchange.Redis;

namespace Ma7MQ.Core.Broker
{
    public class RateLimiter
    {
        private readonly IDatabase _db;
        private readonly double _rate;
        private readonly double _burst;

        public RateLimiter(IDatabase db, double rate, double burst)
        {
            _db = db;
            _rate = rate;
            _burst = burst;
        }

        public async Task<bool> AllowAsync(string clientID)
        {
            string key = $"mq:limiter:{clientID}";
            
            var result = await _db.ScriptEvaluateAsync(@"
                local key = KEYS[1]
                local rate = tonumber(ARGV[1])
                local capacity = tonumber(ARGV[2])
                local now = tonumber(ARGV[3])
                
                local data = redis.call('HMGET', key, 'tokens', 'last_refill')
                local tokens = tonumber(data[1])
                local last_refill = tonumber(data[2])
                
                if not tokens then
                    tokens = capacity
                    last_refill = now
                else
                    local elapsed = (now - last_refill) / 10000000.0
                    tokens = tokens + elapsed * rate
                    if tokens > capacity then
                        tokens = capacity
                    end
                end
                
                if tokens >= 1.0 then
                    tokens = tokens - 1.0
                    redis.call('HMSET', key, 'tokens', tokens, 'last_refill', now)
                    redis.call('EXPIRE', key, 60)
                    return 1
                else
                    redis.call('HMSET', key, 'tokens', tokens, 'last_refill', now)
                    redis.call('EXPIRE', key, 60)
                    return 0
                end
            ", new RedisKey[] { key }, new RedisValue[] { _rate, _burst, DateTime.UtcNow.Ticks });

            return (int)result == 1;
        }
    }
}
