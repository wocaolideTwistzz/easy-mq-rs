-- `KEYS[1]` -> easy-mq:`qname`:deadline
-- `KEYS[2]` -> easy-mq:`qname`:stream
-- `KEYS[3]` -> easy-mq:`qname`:scheduled
-- `KEYS[4]` -> easy-mq:`qname`:dependent
-- `KEYS[5]` -> easy-mq:`qname`:archive
-- `KEYS[6]` -> easy-mq:`qname`:archive_stream

-- `ARGV[1]` -> current
-- `ARGV[2]` -> qname

local deadline_key = KEYS[1]
local stream_key = KEYS[2]
local scheduled_key = KEYS[3]
local dependent_key = KEYS[4]
local archive_key = KEYS[5]
local archive_stream_key = KEYS[6]

local current = tonumber(ARGV[1]) or 0
local qname = ARGV[2]

-- easy-mq:`qname`:task:{`task_id`} -> easy-mq:`task`:dependent:{`task_id`}
-- 将`task_key`转换成`dependent_task_key`.
-- Convert `task_key` to `dependent_task_key`.
local function to_dependent_task_key(task_key)
    local task_id = string.sub(task_key, string.len(stream_key))
    return 'easy-mq:' .. qname .. ':dependent:' .. task_id
end

-- 1. 获取过期的任务列表, 限制100个 避免转移时间过长
-- 1. Retrieve task list which expired, limit to 100 to avoid excessively long transfer times.
local tasks = redis.call('ZRANGEBYSCORE', deadline_key, '-inf', current, 'LIMIT', 0, 100)

if #tasks == 0 then
    return 0
end

local xdel_stream_ids = {}
local xack_stream_ids = {}
local zrem_scheduled_task_keys = {}
local to_remove_dep_keys = {}
local to_archive_args = {}
local to_archive_stream_args = {}

-- 移动任务
-- move tasks
for _, task_key in ipairs(tasks) do
    -- 获取任务参数
    -- get task args.
    local task_args = redis.call('HMGET', task_key, 'state', 'retention', 'stream_id')
    local state = task_args[1]
    local retention = tonumber(task_args[2]) or 0
    local stream_id = task_args[3]

    if state == 'pending' then
        -- `pending` 状态下, 任务在 `stream` 消息队列中, 还未被消费者取出, 需要被删除
        -- Tasks in the `pending` state are in the `stream` message queue and have not yet been retrieved by a consumer;
        -- they need to be deleted.
        table.insert(xdel_stream_ids, stream_id)
    elseif state == 'active' then
        -- `active` 状态下, 任务在 `stream` 消息队列中, 但正在被某个消费者消费, 需要标记为完成.
        -- Tasks in the `active` state are in the `stream` message queue, but is being consumed by a consumer;
        -- they need to be mark as completed.
        table.insert(xack_stream_ids, stream_id)
    elseif state == 'scheduled' or state == 'retry' then
        -- `scheduled` 或 `retry` 状态下, 任务在 `scheduled` 队列中, 需要被删除
        -- Tasks in the `scheduled` or `retry` state are in the `scheduled` message queue,
        -- they need to be deleted.
        table.insert(zrem_scheduled_task_keys, task_key)
    elseif state == 'dependent' then
        -- `dependent` 状态下, 任务在 `dependent` 中, 需要被删除
        -- Tasks in the `dependent` state are in the `dependent` queues,
        -- they need to be deleted.
        table.insert(to_remove_dep_keys, to_dependent_task_key(task_key))
    end

    -- 将任务标记为取消
    -- Mark task state as `canceled`
    redis.call('HSET', task_key,
        'state', 'canceled',
        'completed_at', current,
        'last_err_at', current,
        'last_err', 'deadline exceeded'
    )

    local archive_expired_at = current + retention
    table.insert(to_archive_args, archive_expired_at)
    table.insert(to_archive_args, task_key)

    if type(stream_id) == 'string' then
        table.insert(to_archive_stream_args, archive_expired_at)
        table.insert(to_archive_stream_args, stream_id)
    end
end

-- 删除还未被消费的任务. (in `pending` state)
-- Delete tasks that have not yet been consumed. (in `pending` state)
if #xdel_stream_ids > 0 then
    redis.call('XDEL', stream_key, unpack(xdel_stream_ids))
end

-- 标记消费中的任务为完成. (in `active` state)
-- Mark the task in the consumption as completed. (in `active` state)
if #xack_stream_ids > 0 then
    redis.call('XACK', stream_key, 'default', unpack(xack_stream_ids))
end

-- 删除定时任务指定时间还未到达的任务 (in `scheduled`/`retry` state)
-- Delete scheduled tasks that have not yet arrived at the specified time (in `scheduled`/`retry` state)
if #zrem_scheduled_task_keys > 0 then
    redis.call('ZREM', scheduled_key, unpack(zrem_scheduled_task_keys))
end

-- 删除还在等待依赖的任务 (in `dependent` state)
-- Delete tasks that are still waiting for dependencies
if #to_remove_dep_keys > 0 then
    redis.call('SREM', dependent_key, unpack(to_remove_dep_keys))
    redis.call('DEL', unpack(to_remove_dep_keys))
end

-- 存档任务
-- Save tasks
redis.call('ZADD', archive_key, unpack(to_archive_args))
redis.call('ZADD', archive_stream_key, unpack(to_archive_stream_args))

return #tasks
