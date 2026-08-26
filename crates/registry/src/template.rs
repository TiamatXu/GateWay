//! 路径模板。`{name}` 为参数占位符，入站匹配取出的参数按同名传递到上游路径。

use std::collections::HashMap;

use smol_str::SmolStr;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PathTemplate {
    segments: Vec<Segment>,
    params: Vec<SmolStr>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum Segment {
    Literal(String),
    Param(SmolStr),
}

impl PathTemplate {
    /// # Errors
    /// 花括号不配对、或参数名为空时返回错误。
    pub fn parse(src: &str) -> Result<Self, String> {
        let mut segments = Vec::new();
        let mut params = Vec::new();
        let mut rest = src;
        while let Some(open) = rest.find('{') {
            let (lit, tail) = rest.split_at(open);
            if !lit.is_empty() {
                segments.push(Segment::Literal(lit.to_owned()));
            }
            let close = tail
                .find('}')
                .ok_or_else(|| format!("路径模板 {src:?} 的花括号不配对"))?;
            let name = &tail[1..close];
            if name.is_empty() {
                return Err(format!("路径模板 {src:?} 有空的参数名"));
            }
            let name = SmolStr::new(name);
            params.push(name.clone());
            segments.push(Segment::Param(name));
            rest = &tail[close + 1..];
        }
        if rest.contains('}') {
            return Err(format!("路径模板 {src:?} 的花括号不配对"));
        }
        if !rest.is_empty() {
            segments.push(Segment::Literal(rest.to_owned()));
        }
        Ok(Self { segments, params })
    }

    #[must_use]
    pub fn params(&self) -> &[SmolStr] {
        &self.params
    }

    /// 用入站匹配得到的参数渲染上游路径。
    ///
    /// # Errors
    /// 模板引用了 `params` 里没有的参数名时返回错误——这在加载期已校验过，
    /// 运行期出现说明匹配器与模板不一致。
    pub fn render(&self, args: &HashMap<SmolStr, String>) -> Result<String, String> {
        let mut out = String::new();
        for seg in &self.segments {
            match seg {
                Segment::Literal(s) => out.push_str(s),
                Segment::Param(name) => {
                    let v = args
                        .get(name)
                        .ok_or_else(|| format!("路径参数 {name} 缺失"))?;
                    out.push_str(v);
                }
            }
        }
        Ok(out)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn renders_params() {
        let t = PathTemplate::parse("/v1/files/{file_id}/content").unwrap();
        assert_eq!(t.params(), ["file_id"]);
        let args = HashMap::from([(SmolStr::new("file_id"), "file-abc".to_owned())]);
        assert_eq!(t.render(&args).unwrap(), "/v1/files/file-abc/content");
    }

    #[test]
    fn literal_only_template_has_no_params() {
        let t = PathTemplate::parse("/v1/chat/completions").unwrap();
        assert!(t.params().is_empty());
        assert_eq!(t.render(&HashMap::new()).unwrap(), "/v1/chat/completions");
    }

    #[test]
    fn rejects_unbalanced_braces() {
        assert!(PathTemplate::parse("/v1/{file_id").is_err());
        assert!(PathTemplate::parse("/v1/file_id}").is_err());
        assert!(PathTemplate::parse("/v1/{}").is_err());
    }
}
