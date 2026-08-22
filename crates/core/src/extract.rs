use crate::UsageVector;

/// 从响应字节流中增量抽取用量。
///
/// `feed` 与转发同步执行，必须是纯状态机推进：不做 IO、不阻塞，否则拖慢转发延迟。
pub trait UsageExtractor: Send {
    fn feed(&mut self, chunk: &[u8]);

    /// 返回当前已抽取到的用量，不要求流已结束——这是断连结算的关键。
    fn snapshot(&self) -> UsageVector;

    fn finish(self: Box<Self>) -> UsageVector;
}
