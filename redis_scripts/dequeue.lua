-- 从给出的N个队列中取出第一个未被消费的任务
-- Take the first unconsumed task from the given N queues.

-- KEYS -> [stream1, stream2 ...] (stream: easy-mq:{qname}:stream)
-- ARGV[1] -> consumer
-- ARGV[2] -> current timestamp in milliseconds

local consumer = ARGV[1]
local current = ARGV[2]

for i = 1, #KEYS do
    local stream_key = KEYS[i]
    -- 使用默认消费者组 `default` 从队列中取出一个任务
    -- Dequeue a task using the default consumer group `default`
    local msg = redis.call('XREADGROUP', 'GROUP', 'default', consumer, 'COUNT', 1, 'STREAMS', stream_key, '>')
    if msg and #msg > 0 then
        local fields = msg[1][2][1][2]
        for j = 1, #fields, 2 do
            if fields[j] == 'task_key' then
                local task_key = fields[j + 1]
                -- 设置任务状态, 最后开始被消费的时间.
                -- Set task state, last active time.
                redis.call('HSET', task_key,
                    'state', 'active',
                    'last_active_at', current,
                    'last_worker', consumer
                )

                -- 从任务Hash表中返回任务 - easy-mq:{`qname`}:task:{`task_id`}
                -- Return the task from task hash table - easy-mq:{`qname`}:task:{`task_id`}
                return redis.call('HGETALL', task_key)
            end
        end
    end
end

return nil
