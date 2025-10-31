-- `KEYS[1]` -> easy-mq:`qname`:deadline
-- `KEYS[2]` -> easy-mq:`qname`:stream
-- `KEYS[3]` -> easy-mq:`qname`:scheduled
-- `KEYS[4]` -> easy-mq:`qname`:dependent
-- `KEYS[5]` -> easy-mq:`qname`:archive

-- `ARGV[1]` -> current
-- `ARGV[2]` -> qname

local deadline_key = KEYS[1]
local stream_key = KEYS[2]
local scheduled_key = KEYS[3]
local dependent_key = KEYS[4]
local archive_key = KEYS[5]

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
        -- `pending` 状态下, 任务在 `stream` 消息队列中, 还未被消费者取出, 需要直接删除
        table.insert(xdel_stream_ids, stream_id)
    elseif state == 'active' then
        -- `active` 状态下, 任务在 `stream` 消息队列中, 但正在被某个消费者消费, 需要由canceler取消
        table.insert(xack_stream_ids, stream_id)
    elseif state == 'scheduled' or state == 'retry' then
        -- `scheduled` 或 `retry` 状态下, 任务在 `scheduled` 队列中, 需要被删除
        table.insert(zrem_scheduled_task_keys, task_key)
    elseif state == 'dependent' then
        -- `dependent` 状态下, 任务在 `dependent` 中, 需要被删除
        table.insert(to_remove_dep_keys, to_dependent_task_key(task_key))
    end

    table.insert(to_archive_args, current + retention)
    table.insert(task_key)
end

if #xdel_stream_ids > 0 then
    redis.call('XDEL', stream_key, unpack(xdel_stream_ids))
end

if #xack_stream_ids > 0 then
    redis.call('XACK', stream_key, 'default', unpack(xack_stream_ids))
end

if #zrem_scheduled_task_keys > 0 then
    redis.call('ZREM', scheduled_key, unpack(zrem_scheduled_task_keys))
end

if #to_remove_dep_keys > 0 then
    redis.call('SREM', dependent_key, unpack(to_remove_dep_keys))
    redis.call('DEL', unpack(to_remove_dep_keys))
end

return #tasks
