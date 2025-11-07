-- `KEYS[1]` -> easy-mq:`qname`:task:{`task_id`}
-- `KEYS[2]` -> easy-mq:`qname`:stream
-- `KEYS[3]` -> easy-mq:`qname`:scheduled
-- `KEYS[4]` -> easy-mq:`qname`:archive
-- `KEYS[5]` -> easy-mq:`qname`:deadline
-- `KEYS[6]` -> easy-mq:`qname`:archive_stream

-- `ARGV[1]` -> stream id
-- `ARGV[2]` -> current timestamp (in milliseconds)
-- `ARGV[3]` -> task error message

local task_key = KEYS[1]
local stream_key = KEYS[2]
local scheduled_key = KEYS[3]
local archive_key = KEYS[4]
local deadline_key = KEYS[5]
local archive_stream_key = KEYS[6]

local stream_id = ARGV[1]
local current = tonumber(ARGV[2]) or 0
local err_msg = ARGV[3]

-- 1. 将当前消息队列中的当前任务标记为已完成
-- 1. Mark the current task in current message queue as completed.
if redis.call('XACK', stream_key, 'default', stream_id) == 0 then
    return redis.error_reply("Task not exists")
end

-- 2. 获取任务配置项
-- 2. Get task settings.
local values = redis.call('HMGET', task_key, 'retried', 'max_retries', 'retry_interval', 'retention', 'timeout',
    'deadline')
local retried = tonumber(values[1]) or 0
local max_retries = tonumber(values[2]) or 0
local retry_interval = tonumber(values[3]) or 0
local retention = tonumber(values[4]) or 0
local timeout = tonumber(values[5]) or 0
local deadline = tonumber(values[6]) or 0

if retried < max_retries then
    redis.call('ZADD', archive_stream_key, 0, stream_id)
    -- 3. 需要重试.
    -- 3. need to retry.
    retried = retried + 1
    if retry_interval > 0 then
        -- 重试间隔时间 > 0, 投递至 `scheduled`
        -- If retry_interval > 0, deliver to `scheduled`
        local next_process_at = current + retry_interval
        redis.call('ZADD', scheduled_key, next_process_at, task_key)

        -- 更新任务状态为retry...
        -- Updata task state to retry ...
        redis.call('HSET', task_key,
            'retried', retried,
            'state', 'retry',
            'next_process_at', next_process_at,
            'stream_id', '',
            'last_err_at', current,
            'last_err', err_msg
        )
    else
        -- 没有重试间隔时间, 直接投递至 `stream` 消息队列
        -- If there is no retry interval, deliver to `stream` message queue directly.
        local new_stream_id = redis.call('XADD', stream_key, '*',
            'task_key', task_key,
            'timeout', timeout,
            'max_retries', max_retries,
            'retry_interval', retry_interval,
            'retention', retention
        )

        -- 创建任务队列默认消费者组.
        -- Create a default consumer group for the task queue.
        redis.pcall('XGROUP', 'CREATE', stream_key, 'default', 0)

        -- 更新任务状态为pending...
        -- Update task state to pending...
        redis.call('HSET', task_key,
            'retried', retried,
            'state', 'pending',
            'next_process_at', current,
            'stream_id', new_stream_id,
            'last_err_at', current,
            'err_msg', err_msg,
            'last_pending_at', current,
            'last_err', err_msg
        )
        return new_stream_id
    end
else
    -- 4. 无需重试, 直接存档
    -- 4. No need to retry, archive directly
    local archive_expired_at = current + retention
    redis.call('ZADD', archive_key, archive_expired_at, task_key)
    redis.call('ZADD', archive_stream_key, archive_expired_at, stream_id)
    redis.call('HSET', task_key,
        'state', 'failed',
        'last_err_at', current,
        'last_err', err_msg,
        'completed_at', current,
        'err_msg', err_msg
    )

    if deadline > 0 then
        -- 5. 删除`deadline`
        -- 5. Remove `deadline`
        redis.call('ZREM', deadline_key, task_key)
    end
end

return nil
