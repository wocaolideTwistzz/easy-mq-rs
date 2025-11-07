-- `KEYS[1]` -> easy-mq:`qname`:stream
-- `KEYS[2]` -> easy-mq:`qname`:scheduled
-- `KEYS[3]` -> easy-mq:`qname`:archive
-- `KEYS[4]` -> easy-mq:`qname`:archive_stream

-- `ARGV[1]` -> current
-- `ARGV[2]` -> min idle time

local stream_key = KEYS[1]
local scheduled_key = KEYS[2]
local archive_key = KEYS[3]
local archive_stream_key = KEYS[4]

local current = tonumber(ARGV[1])
local min_idle = tonumber(ARGV[2] or 0)

local retry_count = 0
local canceled_count = 0

local cursor = '0-0'

repeat
    local to_xack_stream_ids = {}
    local to_scheduled_args = {}
    local to_archive_args = {}
    local to_archive_stream_args = {}

    -- 提取在 `active` 状态下持续了超过 `min_idle` 的任务
    -- Extract tasks that have remained in the `active` state for longer than `min_idle`.
    local msg = redis.pcall('XAUTOCLAIM', stream_key, 'default', 'default', min_idle, cursor, 'COUNT', 100)

    if type(msg) == "table" and not msg.err and type(msg[2]) == "table" and #msg[2] > 0 then
        cursor = msg[1]

        for _, entry in ipairs(msg[2]) do
            local stream_id = entry[1]
            local fields = entry[2]

            local kv = {}
            for i = 1, #fields, 2 do
                kv[fields[i]] = fields[i + 1]
            end

            local timeout = tonumber(kv['timeout']) or 0

            if timeout > 0 then
                local last_active_at = tonumber(redis.call('HGET', kv['task_key'], 'last_active_at')) or 0

                -- 任务执行超时
                -- Task execution timeout
                if last_active_at + timeout < current then
                    local max_retries = tonumber(kv['max_retries']) or 0
                    local retried = tonumber(kv['retried']) or 0
                    local task_key = kv['task_key']
                    local retention = tonumber(kv['retention']) or 0

                    table.insert(to_xack_stream_ids, stream_id)

                    if retried < max_retries then
                        retried = retried + 1
                        retry_count = retry_count + 1

                        -- stream_id存档,可以直接被删除
                        -- Save stream_id, it could be delete directly
                        table.insert(to_archive_stream_args, 0)
                        table.insert(to_archive_stream_args, stream_id)

                        -- 任务还有重试次数, 重新投递至`stream`
                        -- The task still has several retries, resubmit it to `stream`
                        local retry_interval = tonumber(kv['retry_interval']) or 0

                        if retry_interval > 0 then
                            -- 任务存在重试间隔时间, 投递至`scheduled`等待分配
                            -- The task has retry interval, submit it to `scheduled` awaiting allocation
                            local next_process_at = current + retry_interval

                            table.insert(to_scheduled_args, next_process_at)
                            table.insert(to_scheduled_args, task_key)

                            -- 设置任务状态为`retry`
                            -- Set task state to `retry`
                            redis.call('HSET', task_key,
                                'retried', retried,
                                'state', 'retry',
                                'next_process_at', next_process_at,
                                'stream_id', '',
                                'last_err_at', current,
                                'last_err', 'claim timeout triggered')
                        else
                            -- 任务无间隔时间,直接重新投递到当前队列
                            -- The task has no retry interval, resubmit it to current `stream` directly.
                            local new_stream_id = redis.call('XADD', stream_key, '*',
                                'task_key', task_key,
                                'retried', retried,
                                'max_retries', max_retries,
                                'retry_interval', retry_interval,
                                'timeout', timeout,
                                'retention', retention
                            )

                            -- 设置任务状态为`pending`
                            -- Set task state to `pending`
                            redis.call('HSET', task_key,
                                'retried', retried,
                                'state', 'pending',
                                'stream_id', new_stream_id,
                                'last_err_at', current,
                                'last_err', 'claim timeout triggered')
                        end
                    else
                        canceled_count = canceled_count + 1

                        -- 任务无需重试, 直接取消该任务
                        -- The task does not need to be retried; cancel it directly
                        local archive_expired_at = current + retention

                        table.insert(to_archive_args, archive_expired_at)
                        table.insert(to_archive_args, task_key)

                        table.insert(to_archive_stream_args, archive_expired_at)
                        table.insert(to_archive_stream_args, stream_id)
                        -- 设置任务状态为 `canceled`
                        -- Set task state to `canceled`
                        redis.call('HSET', task_key,
                            'state', 'canceled',
                            'completed_at', current,
                            'last_err_at', current,
                            'last_err', 'claim timeout triggered'
                        )
                    end
                end
            end
        end
    end

    if #to_xack_stream_ids > 0 then
        -- 将旧的或者需要存档的 stream_id 标记为完成
        -- Mark the old or archive stream_id as complete.
        redis.call('XACK', stream_key, 'default', unpack(to_xack_stream_ids))
    end

    if #to_scheduled_args > 0 then
        -- 存储需要在未来某个时间重试的任务
        -- Store requires tasks to be retried at some future time.
        redis.call('ZADD', scheduled_key, unpack(to_scheduled_args))
    end

    if #to_archive_args > 0 then
        -- 存储被取消的任务
        -- Store canceled tasks.
        redis.call('ZADD', archive_key, unpack(to_archive_args))
    end
    if #to_archive_stream_args > 0 then
        redis.call('ZADD', archive_stream_key, unpack(to_archive_stream_args))
    end
until cursor == '0-0'

-- 返回超时后进入重试状态的任务，和超时后被取消的任务
-- Returns the number of tasks that entered the `retry` state after timeout,
-- and the number of tasks that were canceled after timeout.
return { retry_count, canceled_count }
