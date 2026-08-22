use crate::UsageVector;

/// 从响应字节流中增量抽取用量。
///
/// `feed` 与转发同步执行，必须是纯状态机推进：不做 IO、不阻塞，否则拖慢转发延迟。
pub trait UsageExtractor: Send {
    fn feed(&mut self, chunk: &[u8]);

    /// 返回当前已抽取到的用量，不要求流已结束——这是断连结算的关键。
    fn snapshot(&self) -> UsageVector;

    fn finish(self: Box<Self>) -> UsageVector;

    /// 用量是否为估算值。上游未返回权威 usage 时（典型场景是客户端中途断连，
    /// 末帧从未到达）用量由 tokenizer 估算得出，账单必须明示，否则计费争议
    /// 无法追溯举证。
    fn estimated(&self) -> bool {
        false
    }
}
