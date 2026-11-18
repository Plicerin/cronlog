use crate::clock;
use crate::error::{CronlogError, Result};
use chrono::{Duration, Local, NaiveDateTime, NaiveTime, TimeZone};

#[derive(Debug, Clone)]
pub enum Schedule {
    EverySeconds(i64),
    /// A time of day on this machine's clock.
    DailyAt(NaiveTime),
}

/// All times in and out are UTC. Intervals are real elapsed time, so daylight-saving
/// changes do not stretch or shrink them; a daily time is read in the local time zone.
impl Schedule {
    pub fn next_after(&self, after: NaiveDateTime) -> Result<NaiveDateTime> {
        self.next_after_in(&Local, after)
    }

    /// The first slot after `now` in the series that continues from `previous`, so
    /// intervals keep their phase. Constant time however far behind `previous` is.
    pub fn first_after(
        &self,
        previous: NaiveDateTime,
        now: NaiveDateTime,
    ) -> Result<NaiveDateTime> {
        self.first_after_in(&Local, previous, now)
    }

    /// `next_after` with daily times read in `tz`.
    pub fn next_after_in<Tz: TimeZone>(
        &self,
        tz: &Tz,
        after: NaiveDateTime,
    ) -> Result<NaiveDateTime> {
        match self {
            Schedule::EverySeconds(seconds) => {
                if *seconds <= 0 {
                    return Err(CronlogError::InvalidSchedule(
                        "interval must be positive".into(),
                    ));
                }
                Ok(after + Duration::seconds(*seconds))
            }
            Schedule::DailyAt(time) => {
                // Today's slot if it is still ahead, else tomorrow's; a day is 23 to 25
                // hours long, so the third day is only a guard.
                let today = tz.from_utc_datetime(&after).date_naive();
                today
                    .iter_days()
                    .take(3)
                    .map(|day| clock::local_to_utc(tz, day.and_time(*time)))
                    .find(|slot| *slot > after)
                    .ok_or_else(|| CronlogError::InvalidSchedule("invalid date".into()))
            }
        }
    }

    /// `first_after` with daily times read in `tz`.
    pub fn first_after_in<Tz: TimeZone>(
        &self,
        tz: &Tz,
        previous: NaiveDateTime,
        now: NaiveDateTime,
    ) -> Result<NaiveDateTime> {
        let next = self.next_after_in(tz, previous)?;
        if next > now {
            return Ok(next);
        }
        match self {
            Schedule::EverySeconds(seconds) => {
                // now >= previous + seconds here, so the elapsed time is positive and
                // whole seconds round down: previous + (k + 1) * seconds > now.
                let skipped = (now - previous).num_seconds() / seconds;
                Ok(previous + Duration::seconds((skipped + 1) * seconds))
            }
            // A daily slot's phase is its time of day.
            Schedule::DailyAt(_) => self.next_after_in(tz, now),
        }
    }
}

pub fn parse_schedule(input: &str) -> Result<Schedule> {
    let normalized = input.trim().to_lowercase();
    let parts: Vec<&str> = normalized.split_whitespace().collect();

    if parts.len() == 3 && parts[0] == "every" {
        let n: i64 = parts[1]
            .parse()
            .map_err(|_| CronlogError::InvalidSchedule(format!("expected number in '{input}'")))?;
        if n <= 0 {
            return Err(CronlogError::InvalidSchedule(
                "interval must be positive".into(),
            ));
        }
        let seconds = match parts[2] {
            "second" | "seconds" => n,
            "minute" | "minutes" => n * 60,
            "hour" | "hours" => n * 60 * 60,
            other => {
                return Err(CronlogError::InvalidSchedule(format!(
                    "unsupported interval unit '{other}'"
                )))
            }
        };
        return Ok(Schedule::EverySeconds(seconds));
    }

    if parts.len() == 3 && parts[0] == "daily" && parts[1] == "at" {
        let time = NaiveTime::parse_from_str(parts[2], "%H:%M")
            .map_err(|_| CronlogError::InvalidSchedule(format!("expected HH:MM in '{input}'")))?;
        return Ok(Schedule::DailyAt(time));
    }

    Err(CronlogError::InvalidSchedule(format!(
        "'{input}'. supported: 'every N seconds/minutes/hours' or 'daily at HH:MM'"
    )))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn dt(s: &str) -> NaiveDateTime {
        NaiveDateTime::parse_from_str(s, "%Y-%m-%d %H:%M:%S%.f").expect("datetime")
    }

    /// The slot-by-slot walk `first_after` replaces.
    fn walk(schedule: &Schedule, previous: NaiveDateTime, now: NaiveDateTime) -> NaiveDateTime {
        let mut next = schedule.next_after(previous).unwrap();
        while next <= now {
            next = schedule.next_after(next).unwrap();
        }
        next
    }

    #[test]
    fn first_after_matches_walking_slot_by_slot() {
        let previous = dt("2026-11-17 12:00:00.250");
        for schedule in [
            Schedule::EverySeconds(1),
            Schedule::EverySeconds(7),
            Schedule::EverySeconds(600),
            parse_schedule("daily at 07:00").unwrap(),
        ] {
            for now in [
                "2026-11-17 11:59:59",
                "2026-11-17 12:00:00.250",
                "2026-11-17 12:29:59.900",
                "2026-11-17 12:30:00.250",
                "2026-11-17 12:35:00",
                "2026-11-19 06:59:59",
                "2026-11-19 07:00:00",
            ] {
                let now = dt(now);
                assert_eq!(
                    schedule.first_after(previous, now).unwrap(),
                    walk(&schedule, previous, now),
                    "{schedule:?} previous {previous} now {now}"
                );
            }
        }
    }

    #[test]
    fn first_after_catches_up_a_year_at_once() {
        let schedule = Schedule::EverySeconds(1);
        let previous = dt("2025-11-17 12:00:00.500");
        let now = dt("2026-11-17 12:00:00");
        assert_eq!(
            schedule.first_after(previous, now).unwrap(),
            dt("2026-11-17 12:00:00.500")
        );
    }

    mod daylight_saving {
        use super::*;
        use chrono_tz::America::New_York;

        fn utc(s: &str) -> NaiveDateTime {
            dt(s)
        }

        fn daily(time: &str) -> Schedule {
            parse_schedule(&format!("daily at {time}")).unwrap()
        }

        /// The slots after `from`, `n` of them, stepping as the daemon does.
        fn slots(schedule: &Schedule, from: NaiveDateTime, n: usize) -> Vec<NaiveDateTime> {
            let mut out = Vec::new();
            let mut next = from;
            for _ in 0..n {
                next = schedule.next_after_in(&New_York, next).unwrap();
                out.push(next);
            }
            out
        }

        #[test]
        fn a_daily_job_keeps_its_wall_clock_time_across_both_changes() {
            // 07:00 New York is 11:00 UTC in summer time and 12:00 UTC in winter.
            assert_eq!(
                slots(&daily("07:00"), utc("2026-10-31 00:00:00"), 3),
                vec![
                    utc("2026-10-31 11:00:00"),
                    utc("2026-11-01 12:00:00"), // the fall-back day is 25 hours long
                    utc("2026-11-02 12:00:00"),
                ]
            );
            assert_eq!(
                slots(&daily("07:00"), utc("2026-03-07 00:00:00"), 3),
                vec![
                    utc("2026-03-07 12:00:00"),
                    utc("2026-03-08 11:00:00"), // the spring-forward day is 23
                    utc("2026-03-09 11:00:00"),
                ]
            );
        }

        #[test]
        fn a_daily_job_in_the_repeated_hour_runs_once() {
            // 01:30 happens twice on 2026-11-01; the job runs at the first.
            assert_eq!(
                slots(&daily("01:30"), utc("2026-11-01 04:00:00"), 2),
                vec![utc("2026-11-01 05:30:00"), utc("2026-11-02 06:30:00")]
            );
            // Asked again from inside the second 01:xx, it waits for tomorrow.
            assert_eq!(
                daily("01:30")
                    .next_after_in(&New_York, utc("2026-11-01 06:10:00"))
                    .unwrap(),
                utc("2026-11-02 06:30:00")
            );
        }

        #[test]
        fn a_daily_job_in_the_skipped_hour_runs_when_the_clock_jumps() {
            // 02:30 does not exist on 2026-03-08; the job runs at 03:00 EDT (07:00 UTC).
            assert_eq!(
                slots(&daily("02:30"), utc("2026-03-08 05:00:00"), 2),
                vec![utc("2026-03-08 07:00:00"), utc("2026-03-09 06:30:00")]
            );
        }

        #[test]
        fn an_interval_job_keeps_an_even_pace_through_both_changes() {
            let every_5 = Schedule::EverySeconds(300);
            for start in ["2026-11-01 05:30:00", "2026-03-08 06:30:00"] {
                let start = utc(start);
                let runs = slots(&every_5, start, 24);
                assert!(
                    runs.windows(2).all(|w| w[1] - w[0] == Duration::minutes(5)),
                    "{runs:?}"
                );
                assert_eq!(runs[23], start + Duration::hours(2));
            }
        }

        #[test]
        fn catching_up_a_daily_job_lands_on_the_next_wall_clock_slot() {
            // Last ran 07:00 on 10-30; the daemon was off until 08:00 on 11-01 (winter).
            assert_eq!(
                daily("07:00")
                    .first_after_in(
                        &New_York,
                        utc("2026-10-30 11:00:00"),
                        utc("2026-11-01 13:00:00")
                    )
                    .unwrap(),
                utc("2026-11-02 12:00:00")
            );
        }
    }
}
