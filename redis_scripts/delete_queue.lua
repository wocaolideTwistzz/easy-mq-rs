-- KEYS[1] -> easy-mq:`qname`:stream
-- KEYS[2] -> easy-mq:`qname`:scheduled
-- KEYS[3] -> easy-mq:`qname`:dependent
-- KEYS[4] -> easy-mq:`qname`:archive
-- KEYS[5] -> easy-mq:`qname`:archive_stream
-- KEYS[6] -> easy-mq:`qname`:deadline

local stream_key = KEYS[1]
local scheduled_key = KEYS[2]
local dependent_key = KEYS[3]
local archive_key = KEYS[4]
local archive_stream_key = KEYS[5]
local deadline_key = KEYS[6]

local qname = ARGV[1]

local function delete_zset(key)
    while true do
        local task_keys = {}
        local ret = redis.call('ZPOPMIN', key, 100)
        if #ret == 0 then
            break
        end

        for i = 1, #ret, 2 do
            table.insert(task_keys, ret[i])
        end
        redis.log(redis.LOG_WARNING, 'delete_zset', unpack(task_keys))
        redis.call('DEL', unpack(task_keys))
    end
end

-- easy-mq:`qname`:dependent:{`task_id`} -> easy-mq:`task`:dependent:{`task_id`}
-- 将`dependent_task_key`转换成`task_key`.
-- Convert `dependent_task_key` to `task_key`.
local function to_task_key(dependent_task_key)
    local task_id = string.sub(dependent_task_key, string.len(dependent_key) + 2)
    return 'easy-mq:' .. qname .. ':task:' .. task_id
end

-- 删除`archive`中的任务. (已执行完毕,存档的任务)
-- Delete `archive` tasks. (Tasks that have been executed and archived)
delete_zset(archive_key)
-- 删除`scheduled`中的任务. (已计划但未执行的任务)
-- Delete `scheduled` tasks. (Tasks that have been scheduled but not executed)
delete_zset(scheduled_key)

-- 删除`dependent`中的任务. (还在等待依赖,未执行的任务)
-- Delete `dependent` tasks. (Tasks that are still waiting for dependencies, not executed)
local dependent_task_keys = redis.call('ZRANGE', dependent_key, 0, -1)
local to_del_keys = {}
for i = 1, #dependent_task_keys do
    table.insert(to_del_keys, dependent_task_keys[i])
    table.insert(to_del_keys, to_task_key(dependent_task_keys[i]))
end
table.insert(to_del_keys, dependent_key)
redis.call('DEL', unpack(to_del_keys))

-- 删除`active`中的任务. (正在执行的任务)
-- Delete `active` tasks. (Tasks that are currently being executed)
local cursor = '0-0'
repeat
    local msg = redis.pcall('XAUTOCLAIM', stream_key, 'default', 'default', 0, cursor, 'COUNT', 100)

    if type(msg) == "table" and not msg.err and type(msg[2]) == "table" and #msg[2] > 0 then
        cursor = msg[1]

        local to_del_task_keys = {}
        for _, entry in ipairs(msg[2]) do
            local fields = entry[2]

            for i = 1, #fields, 2 do
                if fields[i] == 'task_key' then
                    table.insert(to_del_task_keys, fields[i + 1])
                    break
                end
            end
        end

        redis.call('DEL', unpack(to_del_task_keys))
    end
until cursor == '0-0'

-- 删除`pending`中的任务. (未执行的任务)
-- Delete `pending` tasks. (Tasks that have not been executed)
while true do
    local msg = redis.pcall('XREADGROUP', 'GROUP', 'default', 'cleaner', 'COUNT', 100, 'STREAMS', stream_key, '>')
    if type(msg) == "table" and not msg.err then
        local to_del_task_keys = {}

        local items = msg[1][2]
        for _, item in pairs(items) do
            local fields = item[2]
            for i = 1, #fields, 2 do
                if fields[i] == 'task_key' then
                    table.insert(to_del_task_keys, fields[i + 1])
                    break
                end
            end
        end

        redis.call('DEL', unpack(to_del_task_keys))
    else
        break
    end
end

-- 删除队列相关key
-- Delete queue related keys
redis.call('DEL', stream_key, scheduled_key, dependent_key, archive_key, archive_stream_key, deadline_key)

return 1
