use super::AuthError;
use axum::http::{
    HeaderMap, HeaderValue,
    header::{COOKIE, SET_COOKIE},
};
use cookie::{Cookie, SameSite, time::Duration};

pub(super) const ACCESS_COOKIE: &str = "__Secure-tackt-access";
pub(super) const REFRESH_COOKIE: &str = "__Secure-tackt-refresh";

pub(super) fn get(headers: &HeaderMap, name: &str) -> Result<Option<String>, AuthError> {
    let mut found = None;
    for header in headers.get_all(COOKIE) {
        let header = header.to_str().map_err(|_| AuthError::Unauthorized)?;
        for cookie in Cookie::split_parse(header).flatten() {
            if cookie.name() != name {
                continue;
            }
            let value = cookie.value();
            if found.is_some() || value.is_empty() || value.len() > 16 * 1024 {
                return Err(AuthError::Unauthorized);
            }
            found = Some(value.to_owned());
        }
    }
    Ok(found)
}

pub(super) fn set(
    headers: &mut HeaderMap,
    access: &str,
    refresh: &str,
    access_seconds: i64,
    refresh_seconds: i64,
) {
    for (name, value, age) in [
        (ACCESS_COOKIE, access, access_seconds),
        (REFRESH_COOKIE, refresh, refresh_seconds),
    ] {
        let cookie = Cookie::build((name, value))
            .path("/api")
            .max_age(Duration::seconds(age.max(0)))
            .http_only(true)
            .secure(true)
            .same_site(SameSite::Strict)
            .build();
        headers.append(
            SET_COOKIE,
            HeaderValue::from_str(&cookie.to_string()).expect("generated token is cookie safe"),
        );
    }
}

pub(super) fn clear(headers: &mut HeaderMap) {
    set(headers, "", "", 0, 0);
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn duplicate_or_empty_auth_cookies_fail_closed() {
        let mut headers = HeaderMap::new();
        headers.insert(
            COOKIE,
            "unrelated=value; __Secure-tackt-access=first"
                .parse()
                .unwrap(),
        );
        assert_eq!(
            get(&headers, ACCESS_COOKIE).unwrap().as_deref(),
            Some("first")
        );
        headers.append(COOKIE, "__Secure-tackt-access=second".parse().unwrap());
        assert!(get(&headers, ACCESS_COOKIE).is_err());
        for value in [
            "__Secure-tackt-access=first; __Secure-tackt-access=second",
            "__Secure-tackt-access=",
        ] {
            headers.insert(COOKIE, value.parse().unwrap());
            assert!(get(&headers, ACCESS_COOKIE).is_err());
        }
    }

    #[test]
    fn issuing_and_clearing_cookies_preserves_security_attributes() {
        let mut headers = HeaderMap::new();
        set(&mut headers, "access", "refresh", 900, 3600);
        for (value, lifetime) in headers.get_all(SET_COOKIE).iter().zip([900, 3600]) {
            let cookie = Cookie::parse(value.to_str().unwrap()).unwrap();
            assert_eq!(cookie.path(), Some("/api"));
            assert_eq!(cookie.http_only(), Some(true));
            assert_eq!(cookie.secure(), Some(true));
            assert_eq!(cookie.same_site(), Some(SameSite::Strict));
            assert_eq!(cookie.max_age(), Some(Duration::seconds(lifetime)));
        }
        headers.clear();
        clear(&mut headers);
        assert_eq!(headers.get_all(SET_COOKIE).iter().count(), 2);
        for value in headers.get_all(SET_COOKIE) {
            let cookie = Cookie::parse(value.to_str().unwrap()).unwrap();
            assert_eq!(cookie.value(), "");
            assert_eq!(cookie.max_age(), Some(Duration::ZERO));
            assert_eq!(cookie.path(), Some("/api"));
        }
    }
}
