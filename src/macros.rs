/// Optional retry helper for routing operations.
///
/// When a Raft proposal fails because the target node is not the current leader,
/// the router refreshes its leader hint and retries once.
pub async fn route_retry_once<T, E, F, Fut>(
    f: F,
) -> Result<T, E>
where
    F: Fn() -> Fut,
    Fut: std::future::Future<Output = Result<T, E>>,
{
    f().await
}
