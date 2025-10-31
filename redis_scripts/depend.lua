-- `KEYS[1]` -> easy-mq:`qname`:task:{`task_id`}
-- `KEYS[2]` -> easy-mq:`qname`:dependent
-- `KEYS[3]` -> easy-mq:`qname`:dependent:{`task_id`}
-- `KEYS[4]` -> easy-mq:`qname`:deadline

-- `ARGV[1]` -> task data
-- `ARGV[2]` -> current timestamp (in milliseconds)
-- `ARGV[3]` -> timeout (in milliseconds)
-- `ARGV[4]` -> deadline timestamp (in milliseconds)
-- `ARGV[5]` -> max retries
-- `ARGV[6]` -> retry interval (in milliseconds)
-- `ARGV[7]` -> retention (in milliseconds)
-- `ARGV[8..]` -> field: task_key; value: task_state

-- 当没有依赖的任务或者依赖参数不正确, 直接返回错误
-- Return error when there is no dependent or dependent parameters are incorrect.
local argv_len = #ARGV
if argv_len < 9 or argv_len % 2 == 0 then
    return redis.error_reply("Invalid number of arguments for `dependent`")
end


local task_key = KEYS[1]

-- 1. 任务已存在,直接返回错误.
-- 1. task already exist, return error_reply directly.
if redis.call('EXISTS', task_key) == 1 then
    return redis.error_reply("Task already exists")
end

local dependent_key = KEYS[2]
local dependent_task_key = KEYS[3]
local deadline_key = KEYS[4]

local task_data = ARGV[1]
local current = tonumber(ARGV[2])
local timeout = tonumber(ARGV[3])
local deadline = tonumber(ARGV[4])
local max_retries = tonumber(ARGV[5])
local retry_interval = tonumber(ARGV[6])
local retention = tonumber(ARGV[7])

-- 2. 存储任务数据
-- 2. Store task data
redis.call('HSET', task_key,
    'state', 'dependent',
    'created_at', current,
    'timeout', timeout,
    'data', task_data,
    'max_retries', max_retries,
    'retry_interval', retry_interval,
    'retention', retention
)

-- 3. 存储当前任务的依赖项.
-- 3. Store dependencies for current task.
local args = { dependent_task_key }
for i = 8, #ARGV do
    table.insert(args, ARGV[i])
end
redis.call('HSET', unpack(args))

-- 4. 挂载当前任务依赖到队列任务依赖列表中
-- 4. Mount the current task dependent to the queue task dependent list
redis.call('SADD', dependent_key, dependent_task_key)

-- 5. 设置任务截止时间(若有设置deadline)
-- 5. Set the task deadline (if a deadline is set)
if deadline > 0 then
    redis.call('ZADD', deadline_key, deadline, task_key)
end

return 1
