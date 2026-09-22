use std::collections::HashMap;
use std::fmt;

/// Maximum allowed length in bytes for a Pub/Sub filter expression.
pub const MAX_FILTER_LENGTH_BYTES: usize = 256;

/// An abstract syntax tree node for a Pub/Sub filter expression.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum FilterExpr {
    /// Checks that the attribute key exists: `attributes:key`
    HasAttribute(String),
    /// Checks exact equality: `attributes.key = "value"`
    ExactMatch { key: String, value: String },
    /// Checks inequality: `attributes.key != "value"` (true if attribute does not exist or value != "value")
    NotEqual { key: String, value: String },
    /// Checks prefix: `hasPrefix(attributes.key, "prefix")`
    HasPrefix { key: String, prefix: String },
    /// Logical NOT: `NOT <expr>`
    Not(Box<FilterExpr>),
    /// Logical AND: `<expr> AND <expr>`
    And(Box<FilterExpr>, Box<FilterExpr>),
    /// Logical OR: `<expr> OR <expr>`
    Or(Box<FilterExpr>, Box<FilterExpr>),
}

impl FilterExpr {
    /// Evaluates whether the given message attributes match this filter expression.
    pub fn matches(&self, attributes: Option<&HashMap<String, String>>) -> bool {
        match self {
            FilterExpr::HasAttribute(key) => {
                attributes.map_or(false, |attrs| attrs.contains_key(key))
            }
            FilterExpr::ExactMatch { key, value } => attributes
                .and_then(|attrs| attrs.get(key))
                .map_or(false, |v| v == value),
            FilterExpr::NotEqual { key, value } => attributes
                .and_then(|attrs| attrs.get(key))
                .map_or(true, |v| v != value),
            FilterExpr::HasPrefix { key, prefix } => attributes
                .and_then(|attrs| attrs.get(key))
                .map_or(false, |v| v.starts_with(prefix)),
            FilterExpr::Not(inner) => !inner.matches(attributes),
            FilterExpr::And(left, right) => {
                left.matches(attributes) && right.matches(attributes)
            }
            FilterExpr::Or(left, right) => left.matches(attributes) || right.matches(attributes),
        }
    }
}

/// Errors that can occur when parsing a Pub/Sub filter expression.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum FilterParseError {
    #[error("Filter expression exceeds maximum length of {MAX_FILTER_LENGTH_BYTES} bytes (actual: {0})")]
    TooLong(usize),
    #[error("Unexpected end of input")]
    UnexpectedEof,
    #[error("Unexpected token '{0}' at position {1}")]
    UnexpectedToken(String, usize),
    #[error("Unclosed string literal starting at position {0}")]
    UnclosedString(usize),
    #[error("Boolean operators must be uppercase ('AND', 'OR', 'NOT'), found '{0}' at position {1}")]
    LowercaseBooleanOperator(String, usize),
    #[error("Expected '{expected}' at position {pos}, found '{found}'")]
    Expected {
        expected: &'static str,
        found: String,
        pos: usize,
    },
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum TokenKind {
    LParen,
    RParen,
    Colon,
    Dot,
    Equal,
    NotEqual,
    Comma,
    And,
    Or,
    Not,
    HasPrefix,
    Attributes,
    StringLit(String),
    Ident(String),
}

#[derive(Debug, Clone)]
struct Token {
    kind: TokenKind,
    pos: usize,
    raw: String,
}

struct Lexer {
    chars: Vec<(usize, char)>,
    cursor: usize,
}

impl Lexer {
    fn new(input: &str) -> Self {
        let chars = input.char_indices().collect();
        Self { chars, cursor: 0 }
    }

    fn peek_char(&self) -> Option<(usize, char)> {
        self.chars.get(self.cursor).copied()
    }

    fn next_char(&mut self) -> Option<(usize, char)> {
        let res = self.chars.get(self.cursor).copied();
        if res.is_some() {
            self.cursor += 1;
        }
        res
    }

    fn tokenize(mut self) -> Result<Vec<Token>, FilterParseError> {
        let mut tokens = Vec::new();

        while let Some((pos, ch)) = self.peek_char() {
            if ch.is_whitespace() {
                self.next_char();
                continue;
            }

            match ch {
                '(' => {
                    self.next_char();
                    tokens.push(Token {
                        kind: TokenKind::LParen,
                        pos,
                        raw: "(".into(),
                    });
                }
                ')' => {
                    self.next_char();
                    tokens.push(Token {
                        kind: TokenKind::RParen,
                        pos,
                        raw: ")".into(),
                    });
                }
                ':' => {
                    self.next_char();
                    tokens.push(Token {
                        kind: TokenKind::Colon,
                        pos,
                        raw: ":".into(),
                    });
                }
                '.' => {
                    self.next_char();
                    tokens.push(Token {
                        kind: TokenKind::Dot,
                        pos,
                        raw: ".".into(),
                    });
                }
                ',' => {
                    self.next_char();
                    tokens.push(Token {
                        kind: TokenKind::Comma,
                        pos,
                        raw: ",".into(),
                    });
                }
                '=' => {
                    self.next_char();
                    tokens.push(Token {
                        kind: TokenKind::Equal,
                        pos,
                        raw: "=".into(),
                    });
                }
                '!' => {
                    self.next_char();
                    if let Some((_, '=')) = self.peek_char() {
                        self.next_char();
                        tokens.push(Token {
                            kind: TokenKind::NotEqual,
                            pos,
                            raw: "!=".into(),
                        });
                    } else {
                        return Err(FilterParseError::UnexpectedToken("!".into(), pos));
                    }
                }
                '"' => {
                    self.next_char();
                    let mut s = String::new();
                    let mut closed = false;
                    while let Some((_, c)) = self.next_char() {
                        if c == '\\' {
                            if let Some((_, escaped)) = self.next_char() {
                                match escaped {
                                    '"' => s.push('"'),
                                    '\\' => s.push('\\'),
                                    'n' => s.push('\n'),
                                    'r' => s.push('\r'),
                                    't' => s.push('\t'),
                                    other => {
                                        s.push('\\');
                                        s.push(other);
                                    }
                                }
                            } else {
                                return Err(FilterParseError::UnclosedString(pos));
                            }
                        } else if c == '"' {
                            closed = true;
                            break;
                        } else {
                            s.push(c);
                        }
                    }
                    if !closed {
                        return Err(FilterParseError::UnclosedString(pos));
                    }
                    tokens.push(Token {
                        kind: TokenKind::StringLit(s.clone()),
                        pos,
                        raw: format!("\"{s}\""),
                    });
                }
                _ if ch.is_ascii_alphanumeric() || ch == '_' || ch == '-' => {
                    let mut ident = String::new();
                    while let Some((_, c)) = self.peek_char() {
                        if c.is_ascii_alphanumeric() || c == '_' || c == '-' {
                            ident.push(c);
                            self.next_char();
                        } else {
                            break;
                        }
                    }

                    let kind = match ident.as_str() {
                        "AND" => TokenKind::And,
                        "OR" => TokenKind::Or,
                        "NOT" => TokenKind::Not,
                        "hasPrefix" => TokenKind::HasPrefix,
                        "attributes" => TokenKind::Attributes,
                        "and" | "or" | "not" => {
                            return Err(FilterParseError::LowercaseBooleanOperator(ident, pos));
                        }
                        _ => TokenKind::Ident(ident.clone()),
                    };

                    tokens.push(Token {
                        kind,
                        pos,
                        raw: ident,
                    });
                }
                _ => {
                    return Err(FilterParseError::UnexpectedToken(ch.to_string(), pos));
                }
            }
        }

        Ok(tokens)
    }
}

struct Parser {
    tokens: Vec<Token>,
    cursor: usize,
}

impl Parser {
    fn new(tokens: Vec<Token>) -> Self {
        Self { tokens, cursor: 0 }
    }

    fn peek(&self) -> Option<&Token> {
        self.tokens.get(self.cursor)
    }

    fn next(&mut self) -> Option<Token> {
        let res = self.tokens.get(self.cursor).cloned();
        if res.is_some() {
            self.cursor += 1;
        }
        res
    }

    fn parse_expression(&mut self) -> Result<FilterExpr, FilterParseError> {
        let expr = self.parse_or()?;
        if let Some(t) = self.peek() {
            return Err(FilterParseError::UnexpectedToken(t.raw.clone(), t.pos));
        }
        Ok(expr)
    }

    fn parse_or(&mut self) -> Result<FilterExpr, FilterParseError> {
        let mut left = self.parse_and()?;
        while let Some(t) = self.peek() {
            if t.kind == TokenKind::Or {
                self.next();
                let right = self.parse_and()?;
                left = FilterExpr::Or(Box::new(left), Box::new(right));
            } else {
                break;
            }
        }
        Ok(left)
    }

    fn parse_and(&mut self) -> Result<FilterExpr, FilterParseError> {
        let mut left = self.parse_not()?;
        while let Some(t) = self.peek() {
            if t.kind == TokenKind::And {
                self.next();
                let right = self.parse_not()?;
                left = FilterExpr::And(Box::new(left), Box::new(right));
            } else {
                break;
            }
        }
        Ok(left)
    }

    fn parse_not(&mut self) -> Result<FilterExpr, FilterParseError> {
        if let Some(t) = self.peek() {
            if t.kind == TokenKind::Not {
                self.next();
                let inner = self.parse_not()?;
                return Ok(FilterExpr::Not(Box::new(inner)));
            }
        }
        self.parse_primary()
    }

    fn parse_primary(&mut self) -> Result<FilterExpr, FilterParseError> {
        let token = self.next().ok_or(FilterParseError::UnexpectedEof)?;
        match token.kind {
            TokenKind::LParen => {
                let inner = self.parse_or()?;
                let close = self.next().ok_or(FilterParseError::UnexpectedEof)?;
                if close.kind != TokenKind::RParen {
                    return Err(FilterParseError::Expected {
                        expected: ")",
                        found: close.raw,
                        pos: close.pos,
                    });
                }
                Ok(inner)
            }
            TokenKind::HasPrefix => {
                let lparen = self.next().ok_or(FilterParseError::UnexpectedEof)?;
                if lparen.kind != TokenKind::LParen {
                    return Err(FilterParseError::Expected {
                        expected: "(",
                        found: lparen.raw,
                        pos: lparen.pos,
                    });
                }

                let key = self.parse_attribute_access()?;

                let comma = self.next().ok_or(FilterParseError::UnexpectedEof)?;
                if comma.kind != TokenKind::Comma {
                    return Err(FilterParseError::Expected {
                        expected: ",",
                        found: comma.raw,
                        pos: comma.pos,
                    });
                }

                let prefix_token = self.next().ok_or(FilterParseError::UnexpectedEof)?;
                let prefix = match prefix_token.kind {
                    TokenKind::StringLit(s) => s,
                    _ => {
                        return Err(FilterParseError::Expected {
                            expected: "string literal",
                            found: prefix_token.raw,
                            pos: prefix_token.pos,
                        });
                    }
                };

                let rparen = self.next().ok_or(FilterParseError::UnexpectedEof)?;
                if rparen.kind != TokenKind::RParen {
                    return Err(FilterParseError::Expected {
                        expected: ")",
                        found: rparen.raw,
                        pos: rparen.pos,
                    });
                }

                Ok(FilterExpr::HasPrefix { key, prefix })
            }
            TokenKind::Attributes => {
                let op = self.next().ok_or(FilterParseError::UnexpectedEof)?;
                match op.kind {
                    TokenKind::Colon => {
                        let key_token = self.next().ok_or(FilterParseError::UnexpectedEof)?;
                        let key = match key_token.kind {
                            TokenKind::Ident(k) | TokenKind::StringLit(k) => k,
                            _ => {
                                return Err(FilterParseError::Expected {
                                    expected: "attribute key",
                                    found: key_token.raw,
                                    pos: key_token.pos,
                                });
                            }
                        };
                        Ok(FilterExpr::HasAttribute(key))
                    }
                    TokenKind::Dot => {
                        let key_token = self.next().ok_or(FilterParseError::UnexpectedEof)?;
                        let key = match key_token.kind {
                            TokenKind::Ident(k) | TokenKind::StringLit(k) => k,
                            _ => {
                                return Err(FilterParseError::Expected {
                                    expected: "attribute key",
                                    found: key_token.raw,
                                    pos: key_token.pos,
                                });
                            }
                        };

                        let cmp_token = self.next().ok_or(FilterParseError::UnexpectedEof)?;
                        match cmp_token.kind {
                            TokenKind::Equal => {
                                let val_token =
                                    self.next().ok_or(FilterParseError::UnexpectedEof)?;
                                let value = match val_token.kind {
                                    TokenKind::StringLit(v) => v,
                                    _ => {
                                        return Err(FilterParseError::Expected {
                                            expected: "string literal",
                                            found: val_token.raw,
                                            pos: val_token.pos,
                                        });
                                    }
                                };
                                Ok(FilterExpr::ExactMatch { key, value })
                            }
                            TokenKind::NotEqual => {
                                let val_token =
                                    self.next().ok_or(FilterParseError::UnexpectedEof)?;
                                let value = match val_token.kind {
                                    TokenKind::StringLit(v) => v,
                                    _ => {
                                        return Err(FilterParseError::Expected {
                                            expected: "string literal",
                                            found: val_token.raw,
                                            pos: val_token.pos,
                                        });
                                    }
                                };
                                Ok(FilterExpr::NotEqual { key, value })
                            }
                            _ => Err(FilterParseError::Expected {
                                expected: "= or !=",
                                found: cmp_token.raw,
                                pos: cmp_token.pos,
                            }),
                        }
                    }
                    _ => Err(FilterParseError::Expected {
                        expected: ": or .",
                        found: op.raw,
                        pos: op.pos,
                    }),
                }
            }
            _ => Err(FilterParseError::UnexpectedToken(token.raw, token.pos)),
        }
    }

    fn parse_attribute_access(&mut self) -> Result<String, FilterParseError> {
        let attr_token = self.next().ok_or(FilterParseError::UnexpectedEof)?;
        if attr_token.kind != TokenKind::Attributes {
            return Err(FilterParseError::Expected {
                expected: "attributes",
                found: attr_token.raw,
                pos: attr_token.pos,
            });
        }

        let dot = self.next().ok_or(FilterParseError::UnexpectedEof)?;
        if dot.kind != TokenKind::Dot {
            return Err(FilterParseError::Expected {
                expected: ".",
                found: dot.raw,
                pos: dot.pos,
            });
        }

        let key_token = self.next().ok_or(FilterParseError::UnexpectedEof)?;
        match key_token.kind {
            TokenKind::Ident(k) | TokenKind::StringLit(k) => Ok(k),
            _ => Err(FilterParseError::Expected {
                expected: "attribute key",
                found: key_token.raw,
                pos: key_token.pos,
            }),
        }
    }
}

/// Parses a Pub/Sub filter expression string into a `FilterExpr`.
pub fn parse_filter(input: &str) -> Result<FilterExpr, FilterParseError> {
    let trimmed = input.trim();
    if trimmed.len() > MAX_FILTER_LENGTH_BYTES {
        return Err(FilterParseError::TooLong(trimmed.len()));
    }

    let tokens = Lexer::new(trimmed).tokenize()?;
    let mut parser = Parser::new(tokens);
    parser.parse_expression()
}

impl fmt::Display for FilterExpr {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            FilterExpr::HasAttribute(key) => write!(f, "attributes:{key}"),
            FilterExpr::ExactMatch { key, value } => write!(f, "attributes.{key} = \"{value}\""),
            FilterExpr::NotEqual { key, value } => write!(f, "attributes.{key} != \"{value}\""),
            FilterExpr::HasPrefix { key, prefix } => {
                write!(f, "hasPrefix(attributes.{key}, \"{prefix}\")")
            }
            FilterExpr::Not(inner) => write!(f, "NOT {inner}"),
            FilterExpr::And(left, right) => write!(f, "({left} AND {right})"),
            FilterExpr::Or(left, right) => write!(f, "({left} OR {right})"),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_has_attribute() {
        let filter = parse_filter("attributes:author").unwrap();
        assert_eq!(filter, FilterExpr::HasAttribute("author".into()));

        let mut attrs = HashMap::new();
        assert!(!filter.matches(Some(&attrs)));

        attrs.insert("author".into(), "Jeff".into());
        assert!(filter.matches(Some(&attrs)));
    }

    #[test]
    fn test_exact_match() {
        let filter = parse_filter("attributes.author = \"unknown\"").unwrap();
        assert_eq!(
            filter,
            FilterExpr::ExactMatch {
                key: "author".into(),
                value: "unknown".into()
            }
        );

        let mut attrs = HashMap::new();
        assert!(!filter.matches(Some(&attrs)));

        attrs.insert("author".into(), "known".into());
        assert!(!filter.matches(Some(&attrs)));

        attrs.insert("author".into(), "unknown".into());
        assert!(filter.matches(Some(&attrs)));
    }

    #[test]
    fn test_not_equal() {
        let filter = parse_filter("attributes.environment != \"staging\"").unwrap();
        assert_eq!(
            filter,
            FilterExpr::NotEqual {
                key: "environment".into(),
                value: "staging".into()
            }
        );

        let mut attrs = HashMap::new();
        // If attribute doesn't exist, it does not equal "staging", so true!
        assert!(filter.matches(Some(&attrs)));

        attrs.insert("environment".into(), "prod".into());
        assert!(filter.matches(Some(&attrs)));

        attrs.insert("environment".into(), "staging".into());
        assert!(!filter.matches(Some(&attrs)));
    }

    #[test]
    fn test_has_prefix() {
        let filter = parse_filter("hasPrefix(attributes.name, \"prefix-val\")").unwrap();
        assert_eq!(
            filter,
            FilterExpr::HasPrefix {
                key: "name".into(),
                prefix: "prefix-val".into()
            }
        );

        let mut attrs = HashMap::new();
        assert!(!filter.matches(Some(&attrs)));

        attrs.insert("name".into(), "prefix-val-123".into());
        assert!(filter.matches(Some(&attrs)));

        attrs.insert("name".into(), "other".into());
        assert!(!filter.matches(Some(&attrs)));
    }

    #[test]
    fn test_complex_and_not_or() {
        let expr_str = "attributes.region = \"us-central1\" AND NOT attributes.environment = \"prod\"";
        let filter = parse_filter(expr_str).unwrap();

        let mut attrs = HashMap::new();
        attrs.insert("region".into(), "us-central1".into());
        attrs.insert("environment".into(), "dev".into());
        assert!(filter.matches(Some(&attrs)));

        attrs.insert("environment".into(), "prod".into());
        assert!(!filter.matches(Some(&attrs)));

        attrs.insert("region".into(), "europe-west1".into());
        assert!(!filter.matches(Some(&attrs)));
    }

    #[test]
    fn test_or_and_precedence() {
        // AND has higher precedence than OR: A OR B AND C -> A OR (B AND C)
        let filter = parse_filter("attributes:a OR attributes:b AND attributes:c").unwrap();
        match filter {
            FilterExpr::Or(a, bc) => {
                assert_eq!(*a, FilterExpr::HasAttribute("a".into()));
                match *bc {
                    FilterExpr::And(b, c) => {
                        assert_eq!(*b, FilterExpr::HasAttribute("b".into()));
                        assert_eq!(*c, FilterExpr::HasAttribute("c".into()));
                    }
                    _ => panic!("Expected And"),
                }
            }
            _ => panic!("Expected Or"),
        }
    }

    #[test]
    fn test_parentheses() {
        let filter = parse_filter("(attributes:a OR attributes:b) AND attributes:c").unwrap();
        match filter {
            FilterExpr::And(ab, c) => {
                assert_eq!(*c, FilterExpr::HasAttribute("c".into()));
                match *ab {
                    FilterExpr::Or(a, b) => {
                        assert_eq!(*a, FilterExpr::HasAttribute("a".into()));
                        assert_eq!(*b, FilterExpr::HasAttribute("b".into()));
                    }
                    _ => panic!("Expected Or"),
                }
            }
            _ => panic!("Expected And"),
        }
    }

    #[test]
    fn test_quoted_attributes_and_escapes() {
        let filter = parse_filter(
            "attributes.\"iana.org/language_tag\" = \"en\" AND attributes.msg = \"hello \\\"world\\\"\"",
        )
        .unwrap();

        let mut attrs = HashMap::new();
        attrs.insert("iana.org/language_tag".into(), "en".into());
        attrs.insert("msg".into(), "hello \"world\"".into());
        assert!(filter.matches(Some(&attrs)));

        attrs.insert("msg".into(), "hello world".into());
        assert!(!filter.matches(Some(&attrs)));
    }

    #[test]
    fn test_lowercase_boolean_error() {
        let err = parse_filter("attributes:a and attributes:b").unwrap_err();
        match err {
            FilterParseError::LowercaseBooleanOperator(op, pos) => {
                assert_eq!(op, "and");
                assert_eq!(pos, 13);
            }
            other => panic!("Expected LowercaseBooleanOperator, got {:?}", other),
        }
    }

    #[test]
    fn test_too_long() {
        let long_str = format!("attributes:{}", "a".repeat(300));
        let err = parse_filter(&long_str).unwrap_err();
        assert!(matches!(err, FilterParseError::TooLong(_)));
    }
}
