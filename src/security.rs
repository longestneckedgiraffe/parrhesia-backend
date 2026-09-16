use axum::{
    body::Body,
    http::{HeaderValue, Request, header},
    middleware::Next,
    response::Response,
};

pub async fn security_headers(request: Request<Body>, next: Next) -> Response {
    let no_store =
        request.uri().path().starts_with("/api/rooms") || request.uri().path().starts_with("/ws/");
    let mut response = next.run(request).await;
    let headers = response.headers_mut();
    if no_store {
        headers.insert(header::CACHE_CONTROL, HeaderValue::from_static("no-store"));
    }

    headers.insert(
        header::CONTENT_SECURITY_POLICY,
        HeaderValue::from_static(
            "default-src 'self'; \
             script-src 'self'; \
             style-src 'self' 'unsafe-inline'; \
             connect-src 'self' wss: ws:; \
             img-src 'self' data: blob:; \
             frame-ancestors 'none'; \
             base-uri 'self'; \
             form-action 'self'",
        ),
    );

    headers.insert(
        header::X_CONTENT_TYPE_OPTIONS,
        HeaderValue::from_static("nosniff"),
    );

    headers.insert(header::X_FRAME_OPTIONS, HeaderValue::from_static("DENY"));

    headers.insert(
        header::REFERRER_POLICY,
        HeaderValue::from_static("strict-origin-when-cross-origin"),
    );

    headers.insert(header::X_XSS_PROTECTION, HeaderValue::from_static("0"));

    headers.insert(
        header::HeaderName::from_static("permissions-policy"),
        HeaderValue::from_static("camera=(self), microphone=()"),
    );

    headers.insert(
        header::STRICT_TRANSPORT_SECURITY,
        HeaderValue::from_static("max-age=31536000; includeSubDomains"),
    );

    response
}

/// Forwarded addresses are accepted only from an explicitly configured proxy.
/// Caddy must overwrite this header, including for direct-origin requests.
pub fn client_ip(
    peer: std::net::IpAddr,
    headers: &axum::http::HeaderMap,
    trusted_proxies: &[std::net::IpAddr],
) -> Result<std::net::IpAddr, axum::http::StatusCode> {
    if !trusted_proxies.contains(&peer) {
        return Ok(peer);
    }
    let mut values = headers.get_all("x-parrhesia-client-ip").iter();
    let value = values.next().ok_or(axum::http::StatusCode::BAD_REQUEST)?;
    if values.next().is_some() {
        return Err(axum::http::StatusCode::BAD_REQUEST);
    }
    value
        .to_str()
        .ok()
        .and_then(|s| s.parse().ok())
        .ok_or(axum::http::StatusCode::BAD_REQUEST)
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::http::{HeaderMap, StatusCode};

    #[test]
    fn forwarded_address_is_trusted_only_from_configured_proxy() {
        let peer = "192.0.2.1".parse().unwrap();
        let forwarded = "198.51.100.5".parse().unwrap();
        let mut headers = HeaderMap::new();
        headers.insert("x-parrhesia-client-ip", "198.51.100.5".parse().unwrap());
        assert_eq!(client_ip(peer, &headers, &[]), Ok(peer));
        assert_eq!(client_ip(peer, &headers, &[peer]), Ok(forwarded));
        headers.insert(
            "x-parrhesia-client-ip",
            "198.51.100.5, 192.0.2.2".parse().unwrap(),
        );
        assert_eq!(
            client_ip(peer, &headers, &[peer]),
            Err(StatusCode::BAD_REQUEST)
        );
        headers.clear();
        assert_eq!(
            client_ip(peer, &headers, &[peer]),
            Err(StatusCode::BAD_REQUEST)
        );
        headers.append("x-parrhesia-client-ip", "198.51.100.5".parse().unwrap());
        headers.append("x-parrhesia-client-ip", "198.51.100.6".parse().unwrap());
        assert_eq!(
            client_ip(peer, &headers, &[peer]),
            Err(StatusCode::BAD_REQUEST)
        );
    }
}
