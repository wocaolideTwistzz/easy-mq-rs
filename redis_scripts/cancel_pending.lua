-- `KEYS[1]` -> easy-mq:`qname`:stream
-- `KEYS[2]` -> easy-mq:`qname`:archive
-- `KEYS[3]` -> easy-mq:`qname`:archive_stream

-- `ARGV[1]` -> min pending time (in milliseconds)

local stream_key = KEYS[1]
local archive_key = KEYS[2]
local archive_stream_key = KEYS[3]

local min_pending = tonumber(ARGV[1]) or 0

-- 查看消费者组信息
-- Lookup stream group information
local msg = redis.pcall('XINFO', 'GROUPS', stream_key)

local function next_deliver_id(last_delivered_id)
    local idx = string.find(last_delivered_id, '-')

    local ts = tonumber(string.sub(last_delivered_id, 0, idx - 1)) or 0
    local i = tonumber(string.sub(last_delivered_id, idx + 1)) or 0

    return ts .. '-' .. i + 1
end

if type(msg) == 'table' and not msg.err and type(msg[1]) == "table" and #msg[1] > 0 then
    local to_xdel_stream_ids = {}
    local to_archive_args = {}
    local to_archive_stream_args = {}

    local fields = msg[1]
    local kv = {}
    for i = 1, #fields, 2 do
        kv[fields[i]] = fields[i + 1]
    end

    local lag = tonumber(kv['lag']) or 0
    -- 队列中存在未被消费者读取的任务
    -- There are tasks in the queue that have not been read by consumers.
    if lag > 0 then
        local next_id = next_deliver_id(kv['last-delivered-id'])
        local current = tonumber(redis.call('time')[1]) * 1000
        local last_id = current - min_pending .. '-0'

        -- 查找`pending`超过指定时间的任务,限制100个
        -- Lookup tasks that are pending for more than a specified time limit 100.
        local tasks = redis.call('XRANGE', stream_key, next_id, last_id, 'COUNT', 100)

        if type(tasks) == 'table' and #tasks > 0 then
            for _, task in ipairs(tasks) do
                local stream_id = task[1]
                local task_fields = task[2]

                table.insert(to_xdel_stream_ids, stream_id)

                local task_kv = {}
                for j = 1, #task_fields, 2 do
                    task_kv[task_fields[j]] = task_fields[j + 1]
                end

                local task_key = task_kv['task_key']
                local retention = tonumber(task_kv['retention']) or 0

                -- 将任务标记为取消
                -- Mark task state as `canceled`
                redis.call('HSET', task_key,
                    'state', 'canceled',
                    'completed_at', current,
                    'last_err_at', current,
                    'last_err', 'cancel pending'
                )

                local archive_expired_at = current + retention
                table.insert(to_archive_stream_args, archive_expired_at)
                table.insert(to_archive_stream_args, stream_id)
                table.insert(to_archive_args, archive_expired_at)
                table.insert(to_archive_args, task_key)
            end
        end
    end

    -- 将取消的任务从队列中移除
    -- Remove canceled tasks from the queue
    if #to_xdel_stream_ids > 0 then
        redis.call('XDEL', stream_key, unpack(to_xdel_stream_ids))
    end

    -- 存档任务
    -- Save the task
    if #to_archive_args > 0 then
        redis.call('ZADD', archive_key, unpack(to_archive_args))
    end

    -- 存档stream_id
    -- Save the stream id
    if #to_archive_stream_args > 0 then
        redis.call('ZADD', archive_stream_key, unpack(to_archive_stream_args))
    end

    -- 返回取消的数量
    -- Returns the number of canceled tasks.
    return #to_xdel_stream_ids
end

return 0
