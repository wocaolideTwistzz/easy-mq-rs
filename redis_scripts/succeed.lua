-- `KEYS[1]` -> easy-mq:`qname`:task:{`task_id`}
-- `KEYS[2]` -> easy-mq:`qname`:stream
-- `KEYS[3]` -> easy-mq:`qname`:archive
-- `KEYS[4]` -> easy-mq:`qname`:deadline

-- `ARGV[1]` -> stream id
-- `ARGV[2]` -> current timestamp (in milliseconds)
-- `ARGV[3]` -> optional: task result

local task_key = KEYS[1]
local stream_key = KEYS[2]
local archive_key = KEYS[3]
local deadline_key = KEYS[4]

local stream_id = ARGV[1]
local current = tonumber(ARGV[2])
local result = ARGV[3]

-- 1. 将当前消息队列中的当前任务标记为已完成
-- 1. Mark the current task in current message queue as completed.
if redis.call('XACK', stream_key, 'default', stream_id) == 0 then
    return redis.error_reply("Task not exists")
end


-- 2. 更新任务数据
-- 2. Update task data
if result == nil then
    redis.call('HSET', task_key,
        'state', 'succeed',
        'completed_at', current
    )
else
    redis.call('HSET', task_key,
        'state', 'succeed',
        'completed_at', current,
        'result', result
    )
end

-- 3. 获取任务存档持续时间, 最后期限
-- 3. Get task archive retention time, deadline
local values = redis.call('HMGET', task_key, 'retention', 'deadline')
local retention = tonumber(values[1]) or 0
local deadline = tonumber(values[2]) or 0

local archive_expired_at = current + retention

-- 4. 更新任务存档过期时间
-- 4. Update task archive expiration time.
redis.call('ZADD', archive_key, archive_expired_at, task_key)

-- 5. 删除`deadline`
-- 5. Remove `deadline`
if deadline > 0 then
    redis.call('ZREM', deadline_key, task_key)
end

return 1
