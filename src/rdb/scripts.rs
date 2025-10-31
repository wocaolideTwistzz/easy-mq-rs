use std::sync::LazyLock;

use deadpool_redis::redis::Script;

/// -- `KEYS[1]` -> easy-mq:`qname`:task:{`task_id`}
/// -- `KEYS[2]` -> easy-mq:`qname`:stream
/// -- `KEYS[3]` -> easy-mq:`qname`:deadline
///
/// -- `ARGV[1]` -> task data
/// -- `ARGV[2]` -> current timestamp (in milliseconds)
/// -- `ARGV[3]` -> timeout (in milliseconds)
/// -- `ARGV[4]` -> deadline timestamp (in milliseconds)
/// -- `ARGV[5]` -> max retries
/// -- `ARGV[6]` -> retry interval (in milliseconds)
/// -- `ARGV[7]` -> retention (in milliseconds)
pub static ENQUEUE: LazyLock<Script> = LazyLock::new(|| {
    Script::new(include_str!(concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/redis_scripts/enqueue.lua"
    )))
});

/// -- KEYS -> [stream1, stream2 ...] (stream: easy-mq:`qname`:stream)
/// -- ARGV[1] -> consumer (worker)
/// -- ARGV[2] -> current timestamp in milliseconds
pub static DEQUEUE: LazyLock<Script> = LazyLock::new(|| {
    Script::new(include_str!(concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/redis_scripts/dequeue.lua"
    )))
});

/// -- `KEYS[1]` -> easy-mq:`qname`:task:{`task_id`}
/// -- `KEYS[2]` -> easy-mq:`qname`:scheduled
/// -- `KEYS[3]` -> easy-mq:`qname`:deadline
///
/// -- `ARGV[1]` -> task data
/// -- `ARGV[2]` -> current timestamp (in milliseconds)
/// -- `ARGV[3]` -> timeout (in milliseconds)
/// -- `ARGV[4]` -> deadline timestamp (in milliseconds)
/// -- `ARGV[5]` -> max retries
/// -- `ARGV[6]` -> retry interval (in milliseconds)
/// -- `ARGV[7]` -> retention (in milliseconds)
/// -- `ARGV[8]` -> scheduled (in milliseconds)
pub static SCHEDULE: LazyLock<Script> = LazyLock::new(|| {
    Script::new(include_str!(concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/redis_scripts/schedule.lua"
    )))
});

/// -- `KEYS[1]` -> easy-mq:`qname`:task:{`task_id`}
/// -- `KEYS[2]` -> easy-mq:`qname`:dependent
/// -- `KEYS[3]` -> easy-mq:`qname`:dependent:{`task_id`}
/// -- `KEYS[4]` -> easy-mq:`qname`:deadline
///
/// -- `ARGV[1]` -> task data
/// -- `ARGV[2]` -> current timestamp (in milliseconds)
/// -- `ARGV[3]` -> timeout (in milliseconds)
/// -- `ARGV[4]` -> deadline timestamp (in milliseconds)
/// -- `ARGV[5]` -> max retries
/// -- `ARGV[6]` -> retry interval (in milliseconds)
/// -- `ARGV[7]` -> retention (in milliseconds)
/// -- `ARGV[8..]` -> field: task_key; value: task_state
pub static DEPEND: LazyLock<Script> = LazyLock::new(|| {
    Script::new(include_str!(concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/redis_scripts/depend.lua"
    )))
});

/// -- `KEYS[1]` -> easy-mq:`qname`:task:{`task_id`}
/// -- `KEYS[2]` -> easy-mq:`qname`:stream
/// -- `KEYS[3]` -> easy-mq:`qname`:archive
/// -- `KEYS[4]` -> easy-mq:`qname`:deadline
///
/// -- `ARGV[1]` -> stream id
/// -- `ARGV[2]` -> current timestamp (in milliseconds)
/// -- `ARGV[3]` -> optional: task result
pub static SUCCEED: LazyLock<Script> = LazyLock::new(|| {
    Script::new(include_str!(concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/redis_scripts/succeed.lua"
    )))
});

/// -- `KEYS[1]` -> easy-mq:`qname`:task:{`task_id`}
/// -- `KEYS[2]` -> easy-mq:`qname`:stream
/// -- `KEYS[3]` -> easy-mq:`qname`:scheduled
/// -- `KEYS[4]` -> easy-mq:`qname`:archive
/// -- `KEYS[5]` -> easy-mq:`qname`:deadline
///
/// -- `ARGV[1]` -> stream id
/// -- `ARGV[2]` -> current timestamp (in milliseconds)
/// -- `ARGV[3]` -> task error message
pub static FAIL: LazyLock<Script> = LazyLock::new(|| {
    Script::new(include_str!(concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/redis_scripts/fail.lua"
    )))
});

/// -- `KEYS[1]` -> easy-mq:`qname`:task:{`task_id`}
/// -- `KEYS[2]` -> easy-mq:`qname`:stream
/// -- `KEYS[3]` -> easy-mq:`qname`:archive
///
/// -- `ARGV[1]` -> stream id
/// -- `ARGV[2]` -> current timestamp (in milliseconds)
/// -- `ARGV[3]` -> optional: task error message
pub static CANCEL: LazyLock<Script> = LazyLock::new(|| {
    Script::new(include_str!(concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/redis_scripts/cancel.lua"
    )))
});

/// -- `KEYS[1]` -> easy-mq:`qname`:scheduled
/// -- `KEYS[2]` -> easy-mq:`qname`:stream
///
/// -- `ARGV[1]` -> current
pub static CLAIM_SCHEDULED: LazyLock<Script> = LazyLock::new(|| {
    Script::new(include_str!(concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/redis_scripts/claim_scheduled.lua"
    )))
});

/// -- `KEYS[1]` -> easy-mq:`qname`:dependent
/// -- `KEYS[2]` -> easy-mq:`qname`:stream
/// -- `KEYS[3]` -> easy-mq:`qname`:archive
///
/// -- `ARGV[1]` -> current
/// -- `ARGV[2]` -> qname
pub static CLAIM_DEPENDENT: LazyLock<Script> = LazyLock::new(|| {
    Script::new(include_str!(concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/redis_scripts/claim_dependent.lua"
    )))
});
