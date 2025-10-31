-- `KEYS[1]` -> easy-mq:`qname`:scheduled
-- `KEYS[2]` -> easy-mq:`qname`:stream

-- `ARGV[1]` -> current

local scheduled_key = KEYS[1]
local stream_key = KEYS[2]

local current = ARGV[1]

-- 1. 获取到期的任务列表, 限制100个 避免转移时间过长
-- 1. Retrieve task list which reaches expiration, limit to 100 to avoid excessively long transfer times.
local tasks = redis.call('ZRANGEBYSCORE', scheduled_key, '-inf', current, 'LIMIT', 0, 100)

if #tasks == 0 then
    return 0
end

-- 移动任务
-- move tasks
for _, task_key in ipairs(tasks) do
    -- 2. 获取任务参数
    -- 2. get task args.
    local task_args = redis.call('HMGET', task_key, 'retried', 'max_retries', 'timeout', 'retry_interval')
    local retried = tonumber(task_args[1]) or 0
    local max_retries = tonumber(task_args[2]) or 0
    local timeout = tonumber(task_args[3]) or 0
    local retry_interval = tonumber(task_args[4]) or 0

    -- 3. 将任务放入 `stream` 消息队列
    -- 3. move task to `stream` message queue
    local new_stream_id = redis.call('XADD', stream_key, '*',
        'task_key', task_key,
        'retried', retried,
        'max_retries', max_retries,
        'retry_interval', retry_interval,
        'last_pending_at', current,
        'timeout', timeout
    )

    -- 4. 将任务状态设置为 `pending`
    -- 4. set task state to `pending`
    redis.call('HSET', task_key, 'state', 'pending', 'stream_id', new_stream_id)
end

-- 5. 创建任务队列默认消费者组.
-- 5. Create a default consumer group for the task queue.
redis.pcall('XGROUP', 'CREATE', stream_key, 'default', 0)

-- 6. 将任务从 scheduled 队列删除
-- 6. remove tasks from scheduled
redis.call('ZREM', scheduled_key, unpack(tasks))

return #tasks
