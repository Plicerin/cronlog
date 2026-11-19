use crate::clock;
use crate::error::{CronlogError, Result};
use chrono::{Datelike, Duration, Local, NaiveDate, NaiveDateTime, NaiveTime, TimeZone, Weekday};

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Schedule {
    EverySeconds(i64),
    /// A time of day on this machine's clock.
    DailyAt(NaiveTime),
    /// A time of day on some days of the week: bit `n` is `n` days after Monday.
    Weekly {
        days: u8,
        time: NaiveTime,
    },
    /// A time of day on one day of the month. A month without that day (the 31st in
    /// April) runs on its last day instead, so `monthly on 31` means month-end.
    Monthly {
        day: u32,
        time: NaiveTime,
    },
}

const SUPPORTED: &str = "supported: 'every N seconds/minutes/hours', 'daily at HH:MM', \
     'weekly on mon,wed,fri at HH:MM' (or mon-fri, weekdays, weekends), \
     'monthly on 1 at HH:MM' (1-31 or last)";

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
                first_slot(tz, after, today.iter_days().take(3), *time)
            }
            Schedule::Weekly { days, time } => {
                if *days == 0 {
                    return Err(CronlogError::InvalidSchedule("no days of the week".into()));
                }
                // Every weekday comes round within 7 days; the extra two are a guard,
                // as for a daily slot.
                let today = tz.from_utc_datetime(&after).date_naive();
                let on_a_day = today
                    .iter_days()
                    .take(9)
                    .filter(|day| days & (1 << day.weekday().num_days_from_monday()) != 0);
                first_slot(tz, after, on_a_day, *time)
            }
            Schedule::Monthly { day, time } => {
                let today = tz.from_utc_datetime(&after).date_naive();
                let months = (0..3).filter_map(|ahead| month_day(today, ahead, *day));
                first_slot(tz, after, months, *time)
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
            // A calendar slot's phase is its time of day.
            Schedule::DailyAt(_) | Schedule::Weekly { .. } | Schedule::Monthly { .. } => {
                self.next_after_in(tz, now)
            }
        }
    }
}

/// The first of `days` at `time` (local, in `tz`) that is after `after` (UTC).
fn first_slot<Tz: TimeZone>(
    tz: &Tz,
    after: NaiveDateTime,
    days: impl Iterator<Item = NaiveDate>,
    time: NaiveTime,
) -> Result<NaiveDateTime> {
    days.map(|day| clock::local_to_utc(tz, day.and_time(time)))
        .find(|slot| *slot > after)
        .ok_or_else(|| CronlogError::InvalidSchedule("no date found for the schedule".into()))
}

/// Day `day` of the month `ahead` months after `today`'s, or that month's last day if it
/// is shorter.
fn month_day(today: NaiveDate, ahead: u32, day: u32) -> Option<NaiveDate> {
    let index = today.year() * 12 + today.month0() as i32 + ahead as i32;
    let (year, month) = (index.div_euclid(12), index.rem_euclid(12) as u32 + 1);
    let first = NaiveDate::from_ymd_opt(year, month, 1)?;
    let last = first
        .checked_add_months(chrono::Months::new(1))?
        .pred_opt()?
        .day();
    NaiveDate::from_ymd_opt(year, month, day.min(last))
}

fn parse_weekday(word: &str) -> Option<Weekday> {
    Some(match word {
        "mon" | "monday" => Weekday::Mon,
        "tue" | "tues" | "tuesday" => Weekday::Tue,
        "wed" | "wednesday" => Weekday::Wed,
        "thu" | "thur" | "thurs" | "thursday" => Weekday::Thu,
        "fri" | "friday" => Weekday::Fri,
        "sat" | "saturday" => Weekday::Sat,
        "sun" | "sunday" => Weekday::Sun,
        _ => return None,
    })
}

/// `mon,wed,fri`, `mon-fri`, `weekdays`, `weekends`, or a mix, as a bit set.
fn parse_days(words: &str, input: &str) -> Result<u8> {
    let bit = |day: Weekday| 1u8 << day.num_days_from_monday();
    let unknown = |word: &str| {
        CronlogError::InvalidSchedule(format!("unknown day '{word}' in '{input}'. {SUPPORTED}"))
    };
    let mut days = 0u8;
    for word in words.split([',', ' ']).filter(|w| !w.is_empty()) {
        days |= match word {
            "weekdays" => 0b0001_1111,
            "weekends" => 0b0110_0000,
            _ => match word.split_once('-') {
                Some((from, to)) => {
                    let from = parse_weekday(from).ok_or_else(|| unknown(word))?;
                    let to = parse_weekday(to).ok_or_else(|| unknown(word))?;
                    // A range wraps through Sunday: fri-mon is Fri, Sat, Sun, Mon.
                    let mut range = bit(from);
                    let mut day = from;
                    while day != to {
                        day = day.succ();
                        range |= bit(day);
                    }
                    range
                }
                None => bit(parse_weekday(word).ok_or_else(|| unknown(word))?),
            },
        };
    }
    if days == 0 {
        return Err(CronlogError::InvalidSchedule(format!(
            "no days given in '{input}'. {SUPPORTED}"
        )));
    }
    Ok(days)
}

/// `1`, `1st`, `22nd`, `31`, or `last` (as 31, the month's last day).
fn parse_month_day(word: &str, input: &str) -> Result<u32> {
    if word == "last" {
        return Ok(31);
    }
    let digits = word.trim_end_matches(|c: char| c.is_ascii_alphabetic());
    let suffix = &word[digits.len()..];
    match (digits.parse::<u32>(), suffix) {
        (Ok(day @ 1..=31), "" | "st" | "nd" | "rd" | "th") => Ok(day),
        _ => Err(CronlogError::InvalidSchedule(format!(
            "expected a day of the month (1-31 or last) in '{input}'"
        ))),
    }
}

fn parse_time(word: &str, input: &str) -> Result<NaiveTime> {
    NaiveTime::parse_from_str(word, "%H:%M")
        .map_err(|_| CronlogError::InvalidSchedule(format!("expected HH:MM in '{input}'")))
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
        return Ok(Schedule::DailyAt(parse_time(parts[2], input)?));
    }

    // weekly on <days> at HH:MM / monthly on <day> at HH:MM
    if parts.len() >= 5 && parts[1] == "on" && parts[parts.len() - 2] == "at" {
        let time = parse_time(parts[parts.len() - 1], input)?;
        let middle = parts[2..parts.len() - 2].join(" ");
        match parts[0] {
            "weekly" => {
                let days = parse_days(&middle, input)?;
                return Ok(Schedule::Weekly { days, time });
            }
            "monthly" if parts.len() == 5 => {
                let day = parse_month_day(&middle, input)?;
                return Ok(Schedule::Monthly { day, time });
            }
            _ => {}
        }
    }

    Err(CronlogError::InvalidSchedule(format!(
        "'{input}'. {SUPPORTED}"
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
            parse_schedule("weekly on tue,thu at 07:00").unwrap(),
            parse_schedule("monthly on 18 at 07:00").unwrap(),
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

    #[test]
    fn weekly_and_monthly_schedules_parse() {
        let at7 = NaiveTime::from_hms_opt(7, 0, 0).unwrap();
        let weekly = |days| Schedule::Weekly { days, time: at7 };
        let parsed = |s: &str| parse_schedule(s).unwrap();
        assert_eq!(parsed("weekly on mon,wed,fri at 07:00"), weekly(0b0010101));
        assert_eq!(parsed("Weekly on Mon, Fri at 07:00"), weekly(0b0010001));
        assert_eq!(parsed("weekly on mon-fri at 07:00"), weekly(0b0011111));
        assert_eq!(parsed("weekly on weekdays at 07:00"), weekly(0b0011111));
        assert_eq!(parsed("weekly on weekends at 07:00"), weekly(0b1100000));
        assert_eq!(parsed("weekly on fri-mon at 07:00"), weekly(0b1110001));
        assert_eq!(parsed("weekly on sunday at 07:00"), weekly(0b1000000));
        assert_eq!(
            parsed("monthly on 1st at 07:00"),
            Schedule::Monthly { day: 1, time: at7 }
        );
        assert_eq!(
            parsed("monthly on last at 07:00"),
            Schedule::Monthly { day: 31, time: at7 }
        );
        for bad in [
            "weekly on funday at 07:00",
            "weekly on at 07:00",
            "weekly on mon at 7am",
            "monthly on 0 at 07:00",
            "monthly on 32 at 07:00",
            "monthly on 1 15 at 07:00",
            "monthly on 3x at 07:00",
            "yearly on 1 at 07:00",
        ] {
            assert!(parse_schedule(bad).is_err(), "{bad}");
        }
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
        fn a_weekly_job_runs_on_its_days_only() {
            // 2026-11-16 is a Monday. Winter: 07:00 New York is 12:00 UTC.
            let job = parse_schedule("weekly on mon,thu at 07:00").unwrap();
            assert_eq!(
                slots(&job, utc("2026-11-16 12:00:00"), 3),
                vec![
                    utc("2026-11-19 12:00:00"), // Thu
                    utc("2026-11-23 12:00:00"), // Mon
                    utc("2026-11-26 12:00:00"), // Thu
                ]
            );
            // Across the fall-back change it keeps 07:00 local.
            let sundays = parse_schedule("weekly on sun at 07:00").unwrap();
            assert_eq!(
                slots(&sundays, utc("2026-10-24 00:00:00"), 2),
                vec![utc("2026-10-25 11:00:00"), utc("2026-11-01 12:00:00")]
            );
        }

        #[test]
        fn a_monthly_job_runs_once_a_month_and_on_short_months_last_day() {
            let job = parse_schedule("monthly on 31 at 07:00").unwrap();
            assert_eq!(
                slots(&job, utc("2027-01-01 00:00:00"), 4),
                vec![
                    utc("2027-01-31 12:00:00"),
                    utc("2027-02-28 12:00:00"),
                    utc("2027-03-31 11:00:00"), // summer time from 03-14
                    utc("2027-04-30 11:00:00"),
                ]
            );
            let first = parse_schedule("monthly on 1 at 07:00").unwrap();
            assert_eq!(
                slots(&first, utc("2026-12-01 13:00:00"), 2),
                vec![utc("2027-01-01 12:00:00"), utc("2027-02-01 12:00:00")]
            );
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
