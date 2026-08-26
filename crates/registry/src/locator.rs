//! 取值位置的表达。
//!
//! 三种前缀构成封闭集合：`$....` 是文档内的 `JSONPath`，`path.<name>` 是路径模板参数，
//! `header.<name>` 是头字段。
//!
//! 解析放在 `Deserialize` 里而非事后校验，是为了让语法错误带上 YAML 的行列位置——
//! 描述文件是给人写的，报错说不清位置等于没有校验。

use std::fmt;
use std::str::FromStr;

use serde::de::{Deserialize, Deserializer, Error as DeError};
use serde_json_path::JsonPath;
use smol_str::SmolStr;

/// 已编译的 `JSONPath`。加载期解析一次，运行期只查询。
#[derive(Debug, Clone)]
pub struct PathExpr {
    src: SmolStr,
    path: JsonPath,
}

impl PathExpr {
    /// # Errors
    /// 不是合法的 RFC 9535 `JSONPath` 时返回错误信息。
    pub fn parse(src: &str) -> Result<Self, String> {
        let path = JsonPath::parse(src).map_err(|e| format!("JSONPath 无效: {e}"))?;
        Ok(Self {
            src: SmolStr::new(src),
            path,
        })
    }

    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.src
    }

    #[must_use]
    pub fn compiled(&self) -> &JsonPath {
        &self.path
    }

    /// 取唯一值。命中多个节点时返回 `None`——用量与句柄字段都要求单值，
    /// 多值命中意味着描述文件写错了路径，宁可不取也不能猜。
    #[must_use]
    pub fn one<'a>(&self, doc: &'a serde_json::Value) -> Option<&'a serde_json::Value> {
        self.path.query(doc).exactly_one().ok()
    }

    /// 取唯一命中节点的归一化位置，供原地改写。多值命中同样返回 `None`。
    #[must_use]
    pub fn one_location(&self, doc: &serde_json::Value) -> Option<Vec<Step>> {
        let located = self.path.query_located(doc);
        let mut it = located.locations();
        let first = it.next()?;
        if it.next().is_some() {
            return None;
        }
        Some(
            first
                .iter()
                .map(|e| {
                    e.as_name().map_or_else(
                        || Step::Index(e.as_index().unwrap_or_default()),
                        |n| Step::Name(n.to_owned()),
                    )
                })
                .collect(),
        )
    }
}

/// 归一化路径的一步。`JSONPath` 查询借着文档，改写要先把位置取成拥有所有权的形式。
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Step {
    Name(String),
    Index(usize),
}

/// 按归一化位置取可变引用。位置来自 `one_location`，中途取不到即返回 `None`。
#[must_use]
pub fn get_mut<'a>(
    doc: &'a mut serde_json::Value,
    steps: &[Step],
) -> Option<&'a mut serde_json::Value> {
    let mut cur = doc;
    for step in steps {
        cur = match step {
            Step::Name(n) => cur.as_object_mut()?.get_mut(n.as_str())?,
            Step::Index(i) => cur.as_array_mut()?.get_mut(*i)?,
        };
    }
    Some(cur)
}

impl PartialEq for PathExpr {
    fn eq(&self, other: &Self) -> bool {
        self.src == other.src
    }
}

impl Eq for PathExpr {}

impl fmt::Display for PathExpr {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.src)
    }
}

impl<'de> Deserialize<'de> for PathExpr {
    fn deserialize<D: Deserializer<'de>>(de: D) -> Result<Self, D::Error> {
        let s = String::deserialize(de)?;
        Self::parse(&s).map_err(D::Error::custom)
    }
}

/// 一次取值的位置。
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Locator {
    /// 文档体内的 `JSONPath`
    Body(PathExpr),
    /// 路径模板参数，如 `path.task_id`
    PathParam(SmolStr),
    /// 头字段，如 `header.x-request-id`
    Header(SmolStr),
}

impl Locator {
    /// 该位置是否指向请求/响应体。非体位置不需要解析 JSON。
    #[must_use]
    pub const fn is_body(&self) -> bool {
        matches!(self, Self::Body(_))
    }

    #[must_use]
    pub fn body_path(&self) -> Option<&PathExpr> {
        match self {
            Self::Body(p) => Some(p),
            _ => None,
        }
    }
}

impl FromStr for Locator {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        if s.starts_with('$') {
            return PathExpr::parse(s).map(Locator::Body);
        }
        if let Some(name) = s.strip_prefix("path.") {
            return non_empty(name).map(|n| Self::PathParam(SmolStr::new(n)));
        }
        if let Some(name) = s.strip_prefix("header.") {
            return non_empty(name).map(|n| Self::Header(SmolStr::new(n.to_ascii_lowercase())));
        }
        Err(format!(
            "取值位置 {s:?} 前缀无法识别，只接受 `$.`（体）、`path.`（路径参数）、`header.`（头）"
        ))
    }
}

fn non_empty(name: &str) -> Result<&str, String> {
    if name.is_empty() {
        Err("取值位置的名字为空".to_owned())
    } else {
        Ok(name)
    }
}

impl fmt::Display for Locator {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Body(p) => write!(f, "{p}"),
            Self::PathParam(n) => write!(f, "path.{n}"),
            Self::Header(n) => write!(f, "header.{n}"),
        }
    }
}

impl<'de> Deserialize<'de> for Locator {
    fn deserialize<D: Deserializer<'de>>(de: D) -> Result<Self, D::Error> {
        let s = String::deserialize(de)?;
        s.parse().map_err(D::Error::custom)
    }
}

#[cfg(feature = "schema")]
mod schema_impl {
    use super::{Locator, PathExpr};

    impl schemars::JsonSchema for PathExpr {
        fn schema_name() -> std::borrow::Cow<'static, str> {
            "JsonPath".into()
        }

        fn json_schema(_: &mut schemars::SchemaGenerator) -> schemars::Schema {
            schemars::json_schema!({
                "type": "string",
                "pattern": r"^\$",
                "description": "RFC 9535 JSONPath，如 $.usage.prompt_tokens"
            })
        }
    }

    impl schemars::JsonSchema for Locator {
        fn schema_name() -> std::borrow::Cow<'static, str> {
            "Locator".into()
        }

        fn json_schema(_: &mut schemars::SchemaGenerator) -> schemars::Schema {
            schemars::json_schema!({
                "type": "string",
                "pattern": r"^(\$|path\.|header\.)",
                "description": "取值位置：$.<jsonpath> | path.<参数名> | header.<头名>"
            })
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rstest::rstest;

    #[rstest]
    #[case("$.usage.prompt_tokens")]
    #[case("$.choices[*].delta.content")]
    fn parses_body_paths(#[case] src: &str) {
        assert!(src.parse::<Locator>().unwrap().is_body());
    }

    #[test]
    fn parses_path_param_and_header() {
        assert_eq!(
            "path.task_id".parse::<Locator>().unwrap(),
            Locator::PathParam("task_id".into())
        );
        // 头名大小写不敏感，统一小写
        assert_eq!(
            "header.X-Request-Id".parse::<Locator>().unwrap(),
            Locator::Header("x-request-id".into())
        );
    }

    #[rstest]
    #[case("usage.tokens")]
    #[case("path.")]
    #[case("$.[")]
    fn rejects_malformed(#[case] src: &str) {
        assert!(src.parse::<Locator>().is_err());
    }

    #[test]
    fn one_location_addresses_the_node_for_rewriting() {
        let mut doc = serde_json::json!({"content": [{"image_url": {"url": "asset://x"}}]});
        let steps = PathExpr::parse("$.content[0].image_url.url")
            .unwrap()
            .one_location(&doc)
            .unwrap();
        *get_mut(&mut doc, &steps).unwrap() = serde_json::json!("asset://y");
        assert_eq!(doc["content"][0]["image_url"]["url"], "asset://y");
    }

    #[test]
    fn one_location_rejects_multi_hit() {
        let doc = serde_json::json!({"a": [{"n": 1}, {"n": 2}]});
        assert!(
            PathExpr::parse("$.a[*].n")
                .unwrap()
                .one_location(&doc)
                .is_none()
        );
    }

    #[test]
    fn one_rejects_multi_hit() {
        let doc = serde_json::json!({"a": [{"n": 1}, {"n": 2}]});
        assert!(PathExpr::parse("$.a[*].n").unwrap().one(&doc).is_none());
        assert_eq!(
            PathExpr::parse("$.a[0].n").unwrap().one(&doc),
            Some(&serde_json::json!(1))
        );
    }
}
