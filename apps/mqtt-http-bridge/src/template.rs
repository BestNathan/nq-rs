use std::collections::HashMap;

use thiserror::Error;

#[derive(Error, Debug, Clone, PartialEq)]
#[allow(dead_code)]
pub enum TemplateError {
    #[error("foreach is not allowed in single mode (batch.size = 1)")]
    ForeachInSingleMode,
    #[error("foreach is required in batch mode (batch.size > 1)")]
    ForeachMissingInBatchMode,
    #[error("missing ${{end}} for ${{foreach}}")]
    MissingEnd,
    #[error("nested ${{foreach}} is not supported")]
    NestedForeach,
}

#[derive(Debug, Clone, PartialEq)]
#[allow(dead_code)]
enum Segment {
    Literal(String),
    Variable(String),
}

#[derive(Debug, Clone)]
#[allow(dead_code)]
struct ForeachBlock {
    separator: String,
    inner: Vec<Segment>,
}

#[derive(Debug, Clone)]
#[allow(dead_code)]
pub struct Template {
    before_foreach: Vec<Segment>,
    foreach_block: Option<ForeachBlock>,
    after_foreach: Vec<Segment>,
}

#[allow(dead_code)]
impl Template {
    pub fn parse(source: &str, is_batch: bool) -> Result<Self, TemplateError> {
        let foreach_start = source.find("${foreach ");

        match (foreach_start, is_batch) {
            (Some(_), false) => Err(TemplateError::ForeachInSingleMode),
            (None, true) => Err(TemplateError::ForeachMissingInBatchMode),
            (None, false) => Ok(Self {
                before_foreach: parse_segments(source),
                foreach_block: None,
                after_foreach: Vec::new(),
            }),
            (Some(start), true) => {
                let before = &source[..start];
                let rest = &source[start..];
                let sep_end = rest.find('}').ok_or(TemplateError::MissingEnd)?;
                let foreach_len = "${foreach ".len();
                let separator = rest[foreach_len..sep_end].to_string();
                let after_marker = &rest[sep_end + 1..];
                let end_pos = after_marker.find("${end}").ok_or(TemplateError::MissingEnd)?;
                let inner_source = &after_marker[..end_pos];
                let after_source = &after_marker[end_pos + "${end}".len()..];

                if inner_source.contains("${foreach ") {
                    return Err(TemplateError::NestedForeach);
                }

                Ok(Self {
                    before_foreach: parse_segments(before),
                    foreach_block: Some(ForeachBlock {
                        separator,
                        inner: parse_segments(inner_source),
                    }),
                    after_foreach: parse_segments(after_source),
                })
            }
        }
    }

    pub fn render_single(&self, vars: &HashMap<String, String>) -> String {
        render_segments(&self.before_foreach, vars)
    }

    pub fn render_batch(&self, messages: &[HashMap<String, String>]) -> String {
        let mut result = String::new();
        result.push_str(&render_segments(&self.before_foreach, &HashMap::new()));

        if let Some(ref fb) = self.foreach_block {
            for (i, vars) in messages.iter().enumerate() {
                if i > 0 {
                    result.push_str(&fb.separator);
                }
                result.push_str(&render_segments(&fb.inner, vars));
            }
        }

        result.push_str(&render_segments(&self.after_foreach, &HashMap::new()));
        result
    }
}

#[allow(dead_code)]
fn render_segments(segments: &[Segment], vars: &HashMap<String, String>) -> String {
    let mut result = String::new();
    for seg in segments {
        match seg {
            Segment::Literal(s) => result.push_str(s),
            Segment::Variable(name) => {
                let value = vars.get(name.as_str()).map(String::as_str).unwrap_or("");
                result.push_str(value);
            }
        }
    }
    result
}

/// Parse source into segments using Peekable iterator (avoids indexing_slicing lint).
#[allow(dead_code)]
fn parse_segments(source: &str) -> Vec<Segment> {
    let mut segments: Vec<Segment> = Vec::new();
    let mut current = String::new();
    let mut chars = source.chars().peekable();

    while let Some(ch) = chars.next() {
        if ch == '$' {
            let mut lookahead = chars.clone();
            if lookahead.next() == Some('{') {
                // Valid ${ — flush current literal
                chars.next(); // consume '{'
                if !current.is_empty() {
                    segments.push(Segment::Literal(std::mem::take(&mut current)));
                }
                // Read variable name until '}'
                let mut var_name = String::new();
                let mut found_close = false;
                while let Some(&next_ch) = chars.peek() {
                    chars.next();
                    if next_ch == '}' {
                        found_close = true;
                        break;
                    }
                    var_name.push(next_ch);
                }
                if found_close && !var_name.is_empty() {
                    segments.push(Segment::Variable(var_name));
                } else {
                    // Incomplete ${ — treat as literal
                    current.push_str("${");
                    current.push_str(&var_name);
                }
            } else {
                current.push('$');
            }
        } else {
            current.push(ch);
        }
    }

    if !current.is_empty() {
        segments.push(Segment::Literal(current));
    }

    segments
}

#[cfg(test)]
mod tests {
    use super::*;

    fn vars(topic: &str, payload: &str) -> HashMap<String, String> {
        let mut m = HashMap::new();
        m.insert("topic".into(), topic.into());
        m.insert("payload".into(), payload.into());
        m.insert("clientid".into(), "client-1".into());
        m.insert("timestamp".into(), "1000".into());
        m
    }

    #[test]
    fn single_plain_text() {
        let t = Template::parse("hello", false).unwrap();
        assert_eq!(t.render_single(&HashMap::new()), "hello");
    }

    #[test]
    fn single_with_variable() {
        let t = Template::parse("{\"t\":\"${topic}\"}", false).unwrap();
        assert_eq!(t.render_single(&vars("test/foo", "bar")), "{\"t\":\"test/foo\"}");
    }

    #[test]
    fn single_missing_var_is_empty() {
        let t = Template::parse("${nope}", false).unwrap();
        assert_eq!(t.render_single(&HashMap::new()), "");
    }

    #[test]
    fn single_rejects_foreach() {
        assert!(matches!(
            Template::parse("[${foreach ,}x${end}]", false),
            Err(TemplateError::ForeachInSingleMode)
        ));
    }

    #[test]
    fn batch_requires_foreach() {
        assert!(matches!(
            Template::parse("no foreach", true),
            Err(TemplateError::ForeachMissingInBatchMode)
        ));
    }

    #[test]
    fn batch_missing_end() {
        assert!(matches!(
            Template::parse("[${foreach ,}no-end", true),
            Err(TemplateError::MissingEnd)
        ));
    }

    #[test]
    fn batch_nested_foreach() {
        assert!(matches!(
            Template::parse("${foreach ,}${foreach ,}x${end}${end}", true),
            Err(TemplateError::NestedForeach)
        ));
    }

    #[test]
    fn batch_two_messages() {
        let t = Template::parse("[${foreach ,}{\"t\":\"${topic}\"}${end}]", true).unwrap();
        let msgs = vec![vars("a", "p1"), vars("b", "p2")];
        assert_eq!(t.render_batch(&msgs), "[{\"t\":\"a\"},{\"t\":\"b\"}]");
    }

    #[test]
    fn batch_empty() {
        let t = Template::parse("[${foreach ,}{\"t\":\"${topic}\"}${end}]", true).unwrap();
        assert_eq!(t.render_batch(&[]), "[]");
    }

    #[test]
    fn bare_dollar_is_literal() {
        let t = Template::parse("$notavar ${topic}", false).unwrap();
        assert_eq!(t.render_single(&vars("t", "p")), "$notavar t");
    }
}
