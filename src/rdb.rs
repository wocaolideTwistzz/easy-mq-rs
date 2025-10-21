use deadpool_redis::redis::aio::ConnectionLike;

use crate::{errors::Result, rdb::scripts::PENDING};

pub async fn pending(conn: &mut impl ConnectionLike) -> Result<()> {
    let x: () = PENDING.arg("arg").invoke_async(conn).await?;

    Ok(())
}

pub mod scripts {
    use std::sync::LazyLock;

    use deadpool_redis::redis::Script;

    pub static PENDING: LazyLock<Script> = LazyLock::new(|| {
        Script::new(include_str!(concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/redis_scripts/pending.lua"
        )))
    });
}
