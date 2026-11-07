//! Allowlisted error metadata; exception messages and arbitrary class names stay private.

pub fn error_category(class: &str) -> &str {
    match class {
        "ApiException" => "ApiException",
        "AttributeError" => "AttributeError",
        "ConnectError" => "ConnectError",
        "ConnectTimeout" => "ConnectTimeout",
        "DatabaseError" => "DatabaseError",
        "DecodingError" => "DecodingError",
        "HTTPStatusError" => "HTTPStatusError",
        "IndexError" => "IndexError",
        "InfluxConfigurationError" => "InfluxConfigurationError",
        "InfluxTransferError" => "InfluxTransferError",
        "IntegrityError" => "IntegrityError",
        "InvalidURL" => "InvalidURL",
        "JSONDecodeError" => "JSONDecodeError",
        "KeyError" => "KeyError",
        "LocalProtocolError" => "LocalProtocolError",
        "OSError" => "OSError",
        "OperationalError" => "OperationalError",
        "OverflowError" => "OverflowError",
        "PoolTimeout" => "PoolTimeout",
        "ReadError" => "ReadError",
        "ReadTimeout" => "ReadTimeout",
        "RemoteProtocolError" => "RemoteProtocolError",
        "RuntimeError" => "RuntimeError",
        "TooManyRedirects" => "TooManyRedirects",
        "TypeError" => "TypeError",
        "UnicodeDecodeError" => "UnicodeDecodeError",
        "UnsupportedProtocol" => "UnsupportedProtocol",
        "ValueError" => "ValueError",
        "WriteError" => "WriteError",
        "WriteTimeout" => "WriteTimeout",
        "_PageProbeDeadlineExceeded" => "_PageProbeDeadlineExceeded",
        "_PageProbeLimitExceeded" => "_PageProbeLimitExceeded",
        "panic" => "panic",
        _ => "Exception",
    }
}

pub fn collector_failure(class: &str) -> String {
    format!("collector failure: {}", error_category(class))
}
