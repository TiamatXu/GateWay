//! 虚拟句柄的文本形态。
//!
//! 客户端拿到的每个上游 ID 都被换成 `gwh_` + 32 位十六进制。带前缀而非裸 UUID，
//! 是因为句柄常常嵌在更长的串里（火山引擎的资产引用写作 `asset://<id>`），
//! 要在任意字符串中把它认出来、换回去，就得有个可扫描的定长记号。
//!
//! 不接受裸上游 ID：允许透传等于放掉归属校验——同一渠道凭证下，
//! 猜到别人的 `task_id` 就能查别人的任务。

use gw_core::HandleId;
use uuid::Uuid;

pub const PREFIX: &str = "gwh_";
const HEX_LEN: usize = 32;
const WIRE_LEN: usize = PREFIX.len() + HEX_LEN;

#[must_use]
pub fn to_wire(id: HandleId) -> String {
    format!("{PREFIX}{}", id.0.simple())
}

/// 整串就是一个虚拟句柄时返回它。
#[must_use]
pub fn parse(s: &str) -> Option<HandleId> {
    let hex = s.strip_prefix(PREFIX)?;
    if hex.len() != HEX_LEN || !hex.bytes().all(|b| b.is_ascii_hexdigit()) {
        return None;
    }
    Uuid::parse_str(hex).ok().map(HandleId)
}

/// 扫描串里出现的全部虚拟句柄，按出现顺序、去重后返回。
#[must_use]
pub fn scan(s: &str) -> Vec<HandleId> {
    let mut out: Vec<HandleId> = Vec::new();
    for (at, _) in s.match_indices(PREFIX) {
        let Some(token) = s.get(at..at + WIRE_LEN) else {
            continue;
        };
        if let Some(id) = parse(token)
            && !out.contains(&id)
        {
            out.push(id);
        }
    }
    out
}

/// 把串里的虚拟句柄换成映射给出的上游 ID。映射返回 `None` 的原样保留——
/// 由调用方决定认不出来是拒绝还是放行，替换本身不做判断。
pub fn replace(s: &str, mut lookup: impl FnMut(HandleId) -> Option<String>) -> String {
    let mut out = String::with_capacity(s.len());
    let mut cursor = 0;
    for (at, _) in s.match_indices(PREFIX) {
        if at < cursor {
            continue;
        }
        let Some(upstream) = s
            .get(at..at + WIRE_LEN)
            .and_then(parse)
            .and_then(&mut lookup)
        else {
            continue;
        };
        out.push_str(&s[cursor..at]);
        out.push_str(&upstream);
        cursor = at + WIRE_LEN;
    }
    out.push_str(&s[cursor..]);
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    fn id(n: u128) -> HandleId {
        HandleId(Uuid::from_u128(n))
    }

    #[test]
    fn round_trips() {
        assert_eq!(parse(&to_wire(id(1))), Some(id(1)));
    }

    #[test]
    fn rejects_lookalikes() {
        assert!(parse("gwh_short").is_none());
        assert!(parse(&format!("gwh_{}", "z".repeat(32))).is_none());
        assert!(parse(&format!("{}x", to_wire(id(1)))).is_none());
        assert!(parse("00000000000000000000000000000001").is_none());
    }

    /// 句柄嵌在更长的串里也要能认出来——`asset://<id>` 是真实用例
    #[test]
    fn finds_handles_embedded_in_a_longer_string() {
        let s = format!("asset://{}", to_wire(id(7)));
        assert_eq!(scan(&s), vec![id(7)]);
        assert_eq!(
            replace(&s, |_| Some("up-7".to_owned())),
            "asset://up-7".to_owned()
        );
    }

    #[test]
    fn replaces_every_occurrence() {
        let s = format!(
            "{} 与 {} 与 {}",
            to_wire(id(1)),
            to_wire(id(2)),
            to_wire(id(1))
        );
        let out = replace(&s, |h| Some(format!("up{}", h.0.as_u128())));
        assert_eq!(out, "up1 与 up2 与 up1");
    }

    #[test]
    fn unknown_handles_are_left_alone() {
        let s = to_wire(id(9));
        assert_eq!(replace(&s, |_| None), s);
    }

    #[test]
    fn scan_deduplicates_in_order() {
        let s = format!("{a} {b} {a}", a = to_wire(id(1)), b = to_wire(id(2)));
        assert_eq!(scan(&s), vec![id(1), id(2)]);
    }
}
