-- `KEYS[1]` -> easy-mq:`qname`:stream
-- `KEYS[2]` -> easy-mq:`qname`:scheduled
-- `KEYS[3]` -> easy-mq:`qname`:dependent
-- `KEYS[4]` -> easy-mq:`qname`:archive

local stream_key = KEYS[1]
local scheduled_key = KEYS[2]
local dependent_key = KEYS[3]
local archive_key = KEYS[4]

-- 获取`stream`队列的消费组信息
-- Get `stream` queue consumer group's message
local msg = redis.pcall('XINFO', 'GROUPS', stream_key)

local pending_count = 0
local active_count = 0

if type(msg) == 'table' and not msg.err and type(msg[1]) == "table" and #msg[1] > 0 then
    local fields = msg[1]
    local kv = {}
    for i = 1, #fields, 2 do
        kv[fields[i]] = fields[i + 1]
    end

    -- 'lag' 代表还未被消费者提取过的任务,对应我们的任务状态 `pending`
    -- 'lag' represents tasks that have not yet been retrieved by consumers, corresponding to our task state `pending`.
    pending_count = tonumber(kv['lag']) or 0

    -- 'pending' 代表任务被消费者取出,但还未响应 ack, 对应我们的任务状态 `active`
    -- 'pending' indicates that the tasks has been retrieved by consumers, but an ACK has not yet been received; this
    -- corresponding to our task state `active`.
    active_count = tonumber(kv['pending']) or 0
end

local scheduled_count = redis.call('ZCARD', scheduled_key)
local dependent_count = redis.call('SCARD', dependent_key)
local completed_count = redis.call('ZCARD', archive_key)

return {
    pending_count,
    active_count,
    scheduled_count,
    dependent_count,
    completed_count
}
