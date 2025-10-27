use std::sync::LazyLock;

use deadpool_redis::redis::Script;

pub static ENQUEUE: LazyLock<Script> = LazyLock::new(|| {
    Script::new(include_str!(concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/redis_scripts/enqueue.lua"
    )))
});

pub static DEQUEUE: LazyLock<Script> = LazyLock::new(|| {
    Script::new(include_str!(concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/redis_scripts/dequeue.lua"
    )))
});
