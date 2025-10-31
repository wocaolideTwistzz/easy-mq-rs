-- `KEYS[1]` -> easy-mq:`qname`:dependent
-- `KEYS[2]` -> easy-mq:`qname`:stream
-- `KEYS[3]` -> easy-mq:`qname`:archive
-- `KEYS[4]` -> easy-mq:`qname`:deadline

-- `ARGV[1]` -> current
-- `ARGV[2]` -> qname

local dependent_key = KEYS[1]
local stream_key = KEYS[2]
local archive_key = KEYS[3]
local deadline_key = KEYS[4]

local current = ARGV[1]
local qname = ARGV[2]

local cursor = '0'

local satisfy_count = 0
local canceled_count = 0
local to_remove_dep_keys = {}
local to_archive_args = {}

-- easy-mq:`qname`:dependent:{`task_id`} -> easy-mq:`task`:dependent:{`task_id`}
-- 将`dependent_task_key`转换成`task_key`.
-- Convert `dependent_task_key` to `task_key`.
local function to_task_key(dependent_task_key)
    local task_id = string.sub(dependent_task_key, string.len(dependent_key) + 2)
    return 'easy-mq:' .. qname .. ':task:' .. task_id
end

-- easy-mq:`qname`:dependent:{`task_id`}
-- 处理需要取消的 `dependent_task_key`.
-- Process `dependent_task_key` that need to be canceled.
local function handle_cancel(dependent_task_key)
    local task_key = to_task_key(dependent_task_key)

    -- 取消计数
    -- canceled count
    canceled_count = canceled_count + 1

    -- 需要从`dependent`中删除
    -- Need to be remove from `dependent`
    table.insert(to_remove_dep_keys, dependent_task_key)

    -- 获取任务存档持续时间, 最后期限
    -- Get task archive retention time, deadline
    local values = redis.call('HMGET', task_key, 'retention', 'deadline')
    local retention = tonumber(values[1]) or 0
    local deadline = tonumber(values[2]) or 0

    local archive_expired_at = current + retention

    -- 需要存档至`archive`
    -- Need to be save to `archive`
    table.insert(to_archive_args, archive_expired_at)
    table.insert(to_archive_args, task_key)

    -- 删除`deadline`
    -- Remove `deadline`
    if deadline > 0 then
        redis.call('ZREM', deadline_key, task_key)
    end

    -- 更新任务状态为`canceled`
    -- Update task state to `canceled`
    redis.call('HSET', task_key,
        'state', 'canceled',
        'completed_at', current,
        'last_err_at', current,
        'last_err', 'Dependence can never be satisfied'
    )
end

-- easy-mq:`qname`:dependent:{`task_id`}
-- 处理满足依赖条件的任务.
-- Process `dependent_task_key` that satisfy the dependencies.
local function handle_satisfy(dependent_task_key)
    local task_key = to_task_key(dependent_task_key)

    -- 满足依赖计数
    -- Satisfying dependent count
    satisfy_count = satisfy_count + 1

    -- 需要从`dependent`中删除
    -- Need to be remove from `dependent`
    table.insert(to_remove_dep_keys, dependent_task_key)

    -- 获取任务参数
    -- get task args.
    local task_args = redis.call('HMGET', task_key, 'retried', 'max_retries', 'timeout', 'retry_interval')
    local retried = tonumber(task_args[1]) or 0
    local max_retries = tonumber(task_args[2]) or 0
    local timeout = tonumber(task_args[3]) or 0
    local retry_interval = tonumber(task_args[4]) or 0

    -- 将任务放入 `stream` 消息队列
    -- move task to `stream` message queue
    local new_stream_id = redis.call('XADD', stream_key, '*',
        'task_key', task_key,
        'retried', retried,
        'max_retries', max_retries,
        'retry_interval', retry_interval,
        'timeout', timeout
    )

    -- 更新任务状态为pending...
    -- Update task state to pending...
    redis.call('HSET', task_key,
        'state', 'pending',
        'stream_id', new_stream_id,
        'last_pending_at', current
    )
end

repeat
    -- 1. 获取需要依赖其他任务的任务列表
    -- 1. Get a list of tasks that dependent on other tasks.
    local res = redis.call('SSCAN', dependent_key, cursor, 'COUNT', 100)
    cursor = res[1]
    local dep_task_list = res[2]

    for _, dependent_task_key in ipairs(dep_task_list) do
        -- 2. 获取当前任务依赖的任务及其完成状态
        -- 2. Get the tasks that the current task depends on and their completion state.
        local dependent_tasks = redis.call('HGETALL', dependent_task_key)

        local completed_tasks = {}
        local should_cancel = false

        -- 3. 检查依赖的任务完成状态是否匹配
        -- 3. Check if the completion state of the dependent tasks matches.
        for i = 1, #dependent_tasks, 2 do
            local required_task_key = dependent_tasks[i]
            local required_task_state = dependent_tasks[i + 1]

            local state = redis.call('HGET', required_task_key, 'state');
            if state == 'canceled' or state == 'succeed' or state == 'failed' then
                if required_task_state == '*' or state == required_task_state then
                    -- 任务完成状态与依赖的状态匹配
                    -- Match task completion state with dependency state.
                    table.insert(completed_tasks, required_task_key)
                else
                    -- 任务完成状态与依赖的状态不匹配, 当前任务的依赖项永远无法完成,需要取消当前任务
                    -- The task completion state does not match the state of its dependencies; the dependencies
                    -- of the current task will never match, the current task needs to be canceled.
                    should_cancel = true
                    break
                end
            end
        end

        if should_cancel then
            handle_cancel(dependent_task_key)
        elseif #completed_tasks == #dependent_tasks / 2 then
            handle_satisfy(dependent_task_key)
        elseif #completed_tasks > 0 then
            -- 删除已经完成的任务.
            -- Delete completed tasks.
            redis.call('HDEL', dependent_task_key, unpack(completed_tasks))
        end
    end
until cursor == '0'

-- 清理`dependent`
-- Clean up `dependent`
if #to_remove_dep_keys > 0 then
    redis.call('DEL', unpack(to_remove_dep_keys))
    redis.call('SREM', dependent_key, unpack(to_remove_dep_keys))
end

-- 更新存档过期时间
-- Update archive timeout
if #to_archive_args > 0 then
    redis.call('ZADD', archive_key, unpack(to_archive_args))
end

-- 创建任务队列默认消费者组.
-- Create a default consumer group for the task queue.
if satisfy_count > 0 then
    redis.pcall('XGROUP', 'CREATE', stream_key, 'default', 0)
end


return { satisfy_count, canceled_count }
