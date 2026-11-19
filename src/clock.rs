//! Cronlog's times. Everything stored or compared is UTC, as a `NaiveDateTime`; local
//! time appears only where people see it (tables, JSON, a job's environment) and where a
//! `daily at HH:MM` schedule names a time of day.
//!
//! Storing local wall-clock time broke at daylight-saving changes: when the clock fell
//! back from 02:00 to 01:00, a job due at "02:00" waited a whole extra hour.

use chrono::{DateTime, Duration, Local, LocalResult, NaiveDateTime, TimeZone, Timelike, Utc};
use serde::Serializer;

/// The current time, UTC.
pub fn now() -> NaiveDateTime {
    Utc::now().naive_utc()
}

/// The UTC instant a local wall-clock time names in `tz`.
///
/// - In the hour a fall-back repeats, the first (earlier) occurrence, so a slot there runs
///   once.
/// - In the gap a spring-forward skips, the moment the clock jumps (02:30 becomes 03:00),
///   so the slot still runs that day, as soon as it can.
pub fn local_to_utc<Tz: TimeZone>(tz: &Tz, local: NaiveDateTime) -> NaiveDateTime {
    if let Some(utc) = earliest_utc(tz, local) {
        return utc;
    }
    // Clocks change on minute boundaries; walk forward to the first minute that exists.
    let mut probe = local
        .with_second(0)
        .and_then(|t| t.with_nanosecond(0))
        .unwrap_or(local);
    for _ in 0..48 * 60 {
        probe += Duration::minutes(1);
        if let Some(utc) = earliest_utc(tz, probe) {
            return utc;
        }
    }
    // No zone has a gap this long; read it as UTC rather than fail.
    local
}

fn earliest_utc<Tz: TimeZone>(tz: &Tz, local: NaiveDateTime) -> Option<NaiveDateTime> {
    match tz.from_local_datetime(&local) {
        LocalResult::Single(t) | LocalResult::Ambiguous(t, _) => Some(t.naive_utc()),
        LocalResult::None => None,
    }
}

/// A stored UTC time in this machine's time zone.
pub fn to_local(utc: NaiveDateTime) -> DateTime<Local> {
    Local.from_utc_datetime(&utc)
}

/// For tables and messages: local time with its offset, e.g. `2026-11-01 01:30:00 -04:00`,
/// so the two 01:30s of a fall-back night can be told apart.
pub fn display(utc: NaiveDateTime) -> String {
    to_local(utc).format("%Y-%m-%d %H:%M:%S%.f %:z").to_string()
}

/// JSON: RFC 3339 local time with offset, e.g. `2026-11-17T12:48:00.5-05:00`. The first 19
/// characters are the local wall-clock time, as they were before times were stored in UTC,
/// so scripts that read `[:19]` as local time keep working.
pub fn serialize_local<S: Serializer>(
    utc: &Option<NaiveDateTime>,
    serializer: S,
) -> std::result::Result<S::Ok, S::Error> {
    match utc {
        Some(utc) => serializer.collect_str(&to_local(*utc).to_rfc3339()),
        None => serializer.serialize_none(),
    }
}

/// Whole seconds as a short duration: `45s`, `12m`, `3h 5m`, `2days 4h`.
pub fn format_seconds(seconds: i64) -> String {
    humantime::format_duration(std::time::Duration::from_secs(seconds.max(0) as u64)).to_string()
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono_tz::America::New_York;

    fn dt(s: &str) -> NaiveDateTime {
        NaiveDateTime::parse_from_str(s, "%Y-%m-%d %H:%M:%S").expect("datetime")
    }

    #[test]
    fn now_is_utc() {
        let drift = (now() - Utc::now().naive_utc()).num_seconds().abs();
        assert!(drift <= 1, "{drift}s from UTC");
    }

    #[test]
    fn ordinary_local_times_convert_by_their_offset() {
        assert_eq!(
            local_to_utc(&New_York, dt("2026-06-14 07:00:00")),
            dt("2026-06-14 11:00:00")
        );
        assert_eq!(
            local_to_utc(&New_York, dt("2026-11-17 07:00:00")),
            dt("2026-11-17 12:00:00")
        );
    }

    #[test]
    fn a_repeated_time_is_its_first_occurrence() {
        // 2026-11-01: 01:30 happens at 05:30 UTC (EDT) and again at 06:30 UTC (EST).
        assert_eq!(
            local_to_utc(&New_York, dt("2026-11-01 01:30:00")),
            dt("2026-11-01 05:30:00")
        );
    }

    #[test]
    fn a_skipped_time_is_the_moment_the_clock_jumps() {
        // 2026-03-08: 02:00 EST jumps to 03:00 EDT, which is 07:00 UTC.
        for skipped in [
            "2026-03-08 02:00:00",
            "2026-03-08 02:30:00",
            "2026-03-08 02:59:59",
        ] {
            assert_eq!(
                local_to_utc(&New_York, dt(skipped)),
                dt("2026-03-08 07:00:00"),
                "{skipped}"
            );
        }
    }

    #[test]
    fn json_keeps_local_wall_time_in_its_first_19_characters() {
        let utc = dt("2026-11-17 17:48:00");
        let json = serde_json::to_string(&Wrapper(Some(utc))).expect("json");
        let local = to_local(utc)
            .naive_local()
            .format("%Y-%m-%dT%H:%M:%S")
            .to_string();
        assert_eq!(&json[1..20], local);
        assert_eq!(serde_json::to_string(&Wrapper(None)).expect("json"), "null");
    }

    #[derive(serde::Serialize)]
    struct Wrapper(#[serde(serialize_with = "serialize_local")] Option<NaiveDateTime>);
}
