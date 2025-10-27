-- 将任务直接入队`pending`, 可以直接被`dequeue`取出消费
-- Put the task directly into `pending` queue, and it can be directly taken out and consumed by `dequeue`

-- `KEYS[1]` -> easy-mq:topics
-- `KEYS[2]` -> easy-mq:{topic}:qname
-- `KEYS[3]` -> easy-mq:{qname}:task:{task_id}
-- `KEYS[4]` -> easy-mq:{qname}:stream
-- `KEYS[5]` -> easy-mq:{qname}:deadline

-- `ARGV[1]` -> topic
-- `ARGV[2]` -> qname
-- `ARGV[3]` -> task data
-- `ARGV[4]` -> current timestamp (in milliseconds)
-- `ARGV[5]` -> timeout (in milliseconds)
-- `ARGV[6]` -> deadline timestamp (in milliseconds)
-- `ARGV[7]` -> max retries
-- `ARGV[8]` -> retry interval (in milliseconds)
-- `ARGV[9]` -> retention (in milliseconds)

local task_key = KEYS[3]

-- 1. 任务已存在,直接返回 '0'.
-- 1. task already exist, return '0' directly.
if redis.call('EXISTS', task_key) == 1 then
    return '0'
end


local topics_key = KEYS[1]
local qname_key = KEYS[2]
local stream_key = KEYS[4]
local deadline_key = KEYS[5]

local topic = ARGV[1]
local qname = ARGV[2]
local task_data = ARGV[3]
local current = tonumber(ARGV[4])
local timeout = tonumber(ARGV[5])
local deadline = tonumber(ARGV[6])
local max_retries = tonumber(ARGV[7])
local retry_interval = tonumber(ARGV[8])
local retention = tonumber(ARGV[9])

-- 2. 将任务放入Pending队列.
-- 2. Put the task into the Pending queue.
local stream_id = redis.call('XADD', stream_key, '*',
    'task_key', task_key,
    'timeout', timeout,
    'max_retries', max_retries,
    'retry_interval', retry_interval
)

-- 3. 创建任务队列默认消费者组.
-- 3. Create a default consumer group for the task queue.
pcall(function()
    redis.call('XGROUP', 'CREATE', stream_key, 'default', 0)
end)

-- 4. 存储任务数据
-- 4. Store task data
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

-- 5. 设置任务截止时间(若有设置deadline)
-- 5. Set the task deadline (if a deadline is set)
if deadline > 0 then
    redis.call('ZADD', deadline_key, deadline, task_key)
end

-- 6. 更新topic产生任务时间
-- 6. Update topic generation task time.
redis.call('ZADD', topics_key, current, topic)

-- 7. 更新topic下的qname
-- 7. Update the qname under the topic
redis.call('HSET', qname_key, qname, current)
return stream_id
