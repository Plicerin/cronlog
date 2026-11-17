use crate::error::{CronlogError, Result};
use chrono::{Duration, NaiveDate, NaiveDateTime, NaiveTime};

#[derive(Debug, Clone)]
pub enum Schedule {
    EverySeconds(i64),
    DailyAt(NaiveTime),
}

impl Schedule {
    pub fn next_after(&self, after: NaiveDateTime) -> Result<NaiveDateTime> {
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
                let today = NaiveDate::from_ymd_opt(
                    after.date().year(),
                    after.date().month(),
                    after.date().day(),
                )
                .ok_or_else(|| CronlogError::InvalidSchedule("invalid date".into()))?;
                let candidate = today.and_time(*time);
                if candidate > after {
                    Ok(candidate)
                } else {
                    Ok((today + Duration::days(1)).and_time(*time))
                }
            }
        }
    }

    /// The first slot after `now` in the series that continues from `previous`, so
    /// intervals keep their phase. Constant time however far behind `previous` is.
    pub fn first_after(
        &self,
        previous: NaiveDateTime,
        now: NaiveDateTime,
    ) -> Result<NaiveDateTime> {
        let next = self.next_after(previous)?;
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
            Schedule::DailyAt(_) => self.next_after(now),
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

trait DateParts {
    fn year(&self) -> i32;
    fn month(&self) -> u32;
    fn day(&self) -> u32;
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
}

impl DateParts for chrono::NaiveDate {
    fn year(&self) -> i32 {
        chrono::Datelike::year(self)
    }
    fn month(&self) -> u32 {
        chrono::Datelike::month(self)
    }
    fn day(&self) -> u32 {
        chrono::Datelike::day(self)
    }
}
