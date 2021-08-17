#  request_rate_limiter.${id}.tokens ，令牌桶剩余令牌数
local tokens_key = KEYS[1]

#  request_rate_limiter.${id}.timestamp ，令牌桶最后填充令牌时间，单位：秒
local timestamp_key = KEYS[2]
--redis.log(redis.LOG_WARNING, "tokens_key " .. tokens_key)

# replenishRate 填充速率
local rate = tonumber(ARGV[1])

# 令牌桶容量
local capacity = tonumber(ARGV[2])

# 得到从 1970-01-01 00:00:00 开始的秒数
local now = tonumber(ARGV[3])

# 消耗令牌桶数量
local requested = tonumber(ARGV[4])

# 计算令牌桶填充满令牌需要多久时间，单位：秒
local fill_time = capacity/rate

# request_rate_limiter.${id}.tokens / request_rate_limiter.${id}.timestamp 的 ttl。 * 2 保证时间充足
local ttl = math.floor(fill_time*2)

--redis.log(redis.LOG_WARNING, "rate " .. ARGV[1])
--redis.log(redis.LOG_WARNING, "capacity " .. ARGV[2])
--redis.log(redis.LOG_WARNING, "now " .. ARGV[3])
--redis.log(redis.LOG_WARNING, "requested " .. ARGV[4])
--redis.log(redis.LOG_WARNING, "filltime " .. fill_time)
--redis.log(redis.LOG_WARNING, "ttl " .. ttl)

# 调用get命令，获得令牌桶剩余令牌数( last_tokens ) ，令牌桶最后填充令牌时间(last_refreshed)
local last_tokens = tonumber(redis.call("get", tokens_key))

if last_tokens == nil then
  last_tokens = capacity
end
--redis.log(redis.LOG_WARNING, "last_tokens " .. last_tokens)


local last_refreshed = tonumber(redis.call("get", timestamp_key))
if last_refreshed == nil then
  last_refreshed = 0
end
--redis.log(redis.LOG_WARNING, "last_refreshed " .. last_refreshed)

#  填充令牌，计算新的令牌桶剩余令牌数( filled_tokens )。填充不超过令牌桶令牌上限
local delta = math.max(0, now-last_refreshed)
local filled_tokens = math.min(capacity, last_tokens+(delta*rate))

# 获取令牌是否成功
local allowed = filled_tokens >= requested
local new_tokens = filled_tokens
local allowed_num = 0
if allowed then
       #  若成功，令牌桶剩余令牌数(new_tokens) 减消耗令牌数( requested )，并设置获取成功( allowed_num = 1 )
  new_tokens = filled_tokens - requested
  allowed_num = 1
end

--redis.log(redis.LOG_WARNING, "delta " .. delta)
--redis.log(redis.LOG_WARNING, "filled_tokens " .. filled_tokens)
--redis.log(redis.LOG_WARNING, "allowed_num " .. allowed_num)
--redis.log(redis.LOG_WARNING, "new_tokens " .. new_tokens)

# 设置令牌桶剩余令牌数( new_tokens ) ，令牌桶最后填充令牌时间(now)
redis.call("setex", tokens_key, ttl, new_tokens)
redis.call("setex", timestamp_key, ttl, now)


# 返回数组结果，[是否获取令牌成功, 剩余令牌数]
return { allowed_num, new_tokens }
