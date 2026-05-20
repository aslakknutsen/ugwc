//! Field-level redaction helpers for log safety.

/// Skip logging raw bearer tokens or PEM-looking bodies in field values.
#[must_use]
pub fn redact_field(input: &str) -> String {
    if input.contains("-----BEGIN") {
        return "<redacted-pem>".into();
    }
    if input.len() > 20
        && input
            .chars()
            .all(|c| c.is_alphanumeric() || c == '-' || c == '_')
    {
        return format!("<redacted:{}>", input.len());
    }
    input.to_string()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn pem_redacted() {
        let s = redact_field("-----BEGIN RSA PRIVATE KEY-----\nMIIF");
        assert_eq!(s, "<redacted-pem>");
    }

    #[test]
    fn long_token_redacted() {
        let tok = "abcdefghijklmnopqrstuvwxyz";
        let s = redact_field(tok);
        assert_eq!(s, format!("<redacted:{}>", tok.len()));
    }

    #[test]
    fn short_string_passthrough() {
        assert_eq!(redact_field("hello"), "hello");
    }
}
