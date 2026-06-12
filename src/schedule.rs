use crate::error::{Cron2Error, Result};
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
                    return Err(Cron2Error::InvalidSchedule(
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
                .ok_or_else(|| Cron2Error::InvalidSchedule("invalid date".into()))?;
                let candidate = today.and_time(*time);
                if candidate > after {
                    Ok(candidate)
                } else {
                    Ok((today + Duration::days(1)).and_time(*time))
                }
            }
        }
    }

    /// For missed runs, Cron2 MVP runs once then computes the next future occurrence.
    pub fn next_future_after(&self, from: NaiveDateTime) -> Result<NaiveDateTime> {
        self.next_after(from)
    }
}

pub fn parse_schedule(input: &str) -> Result<Schedule> {
    let normalized = input.trim().to_lowercase();
    let parts: Vec<&str> = normalized.split_whitespace().collect();

    if parts.len() == 3 && parts[0] == "every" {
        let n: i64 = parts[1]
            .parse()
            .map_err(|_| Cron2Error::InvalidSchedule(format!("expected number in '{input}'")))?;
        if n <= 0 {
            return Err(Cron2Error::InvalidSchedule(
                "interval must be positive".into(),
            ));
        }
        let seconds = match parts[2] {
            "second" | "seconds" => n,
            "minute" | "minutes" => n * 60,
            "hour" | "hours" => n * 60 * 60,
            other => {
                return Err(Cron2Error::InvalidSchedule(format!(
                    "unsupported interval unit '{other}'"
                )))
            }
        };
        return Ok(Schedule::EverySeconds(seconds));
    }

    if parts.len() == 3 && parts[0] == "daily" && parts[1] == "at" {
        let time = NaiveTime::parse_from_str(parts[2], "%H:%M")
            .map_err(|_| Cron2Error::InvalidSchedule(format!("expected HH:MM in '{input}'")))?;
        return Ok(Schedule::DailyAt(time));
    }

    Err(Cron2Error::InvalidSchedule(format!(
        "'{input}'. supported: 'every N seconds/minutes/hours' or 'daily at HH:MM'"
    )))
}

trait DateParts {
    fn year(&self) -> i32;
    fn month(&self) -> u32;
    fn day(&self) -> u32;
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
