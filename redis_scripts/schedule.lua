-- `KEYS[1]` -> easy-mq:`qname`:task:{`task_id`}
-- `KEYS[2]` -> easy-mq:`qname`:scheduled
-- `KEYS[3]` -> easy-mq:`qname`:deadline

-- `ARGV[1]` -> task data
-- `ARGV[2]` -> current timestamp (in milliseconds)
-- `ARGV[3]` -> timeout (in milliseconds)
-- `ARGV[4]` -> deadline timestamp (in milliseconds)
-- `ARGV[5]` -> max retries
-- `ARGV[6]` -> retry interval (in milliseconds)
-- `ARGV[7]` -> retention (in milliseconds)
-- `ARGV[8]` -> scheduled (in milliseconds)


local task_key = KEYS[1]

-- 1. 任务已存在,直接返回 0.
-- 1. task already exist, return 0 directly.
if redis.call('EXISTS', task_key) == 1 then
    return redis.error_reply("Task already exists")
end

local scheduled_key = KEYS[2]
local deadline_key = KEYS[3]

local task_data = ARGV[1]
local current = tonumber(ARGV[2])
local timeout = tonumber(ARGV[3])
local deadline = tonumber(ARGV[4])
local max_retries = tonumber(ARGV[5])
local retry_interval = tonumber(ARGV[6])
local retention = tonumber(ARGV[7])
local scheduled = tonumber(ARGV[8])

-- 2. 存储任务数据
-- 2. Store task data
redis.call('HSET', task_key,
    'state', 'scheduled',
    'created_at', current,
    'timeout', timeout,
    'data', task_data,
    'max_retries', max_retries,
    'retry_interval', retry_interval,
    'next_process_at', scheduled,
    'retention', retention
)

-- 3. 存储任务的预定时间.
-- 3. Store the scheduled time of the task.
redis.call('ZADD', scheduled_key, scheduled, task_key)

-- 4. 设置任务截止时间(若有设置deadline)
-- 4. Set the task deadline (if a deadline is set)
if deadline > 0 then
    redis.call('ZADD', deadline_key, deadline, task_key)
end

return 1
