-- 将任务直接入队`pending`, 可以直接被`dequeue`取出消费
-- Put the task directly into `pending` queue, and it can be directly taken out and consumed by `dequeue`

-- `KEYS[1]` -> easy-mq:`qname`:task:{`task_id`}
-- `KEYS[2]` -> easy-mq:`qname`:stream
-- `KEYS[3]` -> easy-mq:`qname`:deadline

-- `ARGV[1]` -> task data
-- `ARGV[2]` -> current timestamp (in milliseconds)
-- `ARGV[3]` -> timeout (in milliseconds)
-- `ARGV[4]` -> deadline timestamp (in milliseconds)
-- `ARGV[5]` -> max retries
-- `ARGV[6]` -> retry interval (in milliseconds)
-- `ARGV[7]` -> retention (in milliseconds)

local task_key = KEYS[1]

-- 1. 任务已存在,直接返回 '0'.
-- 1. task already exist, return '0' directly.
if redis.call('EXISTS', task_key) == 1 then
    return '0'
end


local stream_key = KEYS[2]
local deadline_key = KEYS[3]

local task_data = ARGV[1]
local current = tonumber(ARGV[2])
local timeout = tonumber(ARGV[3])
local deadline = tonumber(ARGV[4])
local max_retries = tonumber(ARGV[5])
local retry_interval = tonumber(ARGV[6])
local retention = tonumber(ARGV[7])

-- 1. 将任务放入Pending队列.
-- 1. Put the task into the Pending queue.
local stream_id = redis.call('XADD', stream_key, '*',
    'task_key', task_key,
    'timeout', timeout,
    'max_retries', max_retries,
    'retry_interval', retry_interval
)

-- 2. 创建任务队列默认消费者组.
-- 2. Create a default consumer group for the task queue.
pcall(function()
    redis.call('XGROUP', 'CREATE', stream_key, 'default', 0)
end)

-- 3. 存储任务数据
-- 3. Store task data
redis.call('HSET', task_key,
    'state', 'pending',
    'created_at', current,
    'timeout', timeout,
    'data', task_data,
    'max_retries', max_retries,
    'retry_interval', retry_interval,
    'retention', retention,
    'stream_id', stream_id
)

-- 4. 设置任务截止时间(若有设置deadline)
-- 4. Set the task deadline (if a deadline is set)
if deadline > 0 then
    redis.call('ZADD', deadline_key, deadline, task_key)
end

return stream_id
