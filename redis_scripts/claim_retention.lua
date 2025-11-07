-- `KEYS[1]` -> easy-mq:`qname`:archive
-- `KEYS[2]` -> easy-mq:`qname`:archive_stream
-- `KEYS[3]` -> easy-mq:`qname`:stream

-- `ARGV[1]` -> current

local archive_key = KEYS[1]
local archive_stream_key = KEYS[2]
local stream_key = KEYS[3]

local current = ARGV[1]

-- 获取存档超时的任务
-- Find save timeout tasks
local tasks = redis.call('ZRANGEBYSCORE', archive_key, '-INF', current, 'LIMIT', 0, 100)

if #tasks > 0 then
    -- 删除存档超时的任务
    -- Delete save timeout tasks
    redis.call('DEL', unpack(tasks))
end

local stream_ids = redis.call('ZRANGEBYSCORE', archive_stream_key, '-INF', current, 'LIMIT', 0, 100)

if #stream_ids > 0 then
    redis.call('XDEL', stream_key, unpack(stream_ids))
end

-- 返回存档超时的任务数
-- Returns the number of save timeout tasks
return #tasks
