use std::time::{SystemTime, UNIX_EPOCH};

pub fn now_epoch() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|since| since.as_secs() as i64)
        .unwrap_or_default()
}

pub fn parse_stamp(stamp: &str) -> Option<i64> {
    let year = stamp.get(0..4)?.parse::<i64>().ok()?;
    let month = stamp.get(5..7)?.parse::<i64>().ok()?;
    let day = stamp.get(8..10)?.parse::<i64>().ok()?;
    let hour = stamp.get(11..13)?.parse::<i64>().ok()?;
    let minute = stamp.get(14..16)?.parse::<i64>().ok()?;
    let second = stamp.get(17..19)?.parse::<i64>().ok()?;
    Some(days_from_civil(year, month, day) * 86_400 + hour * 3600 + minute * 60 + second)
}

pub fn format_epoch(seconds: i64) -> String {
    let days = seconds.div_euclid(86_400);
    let rest = seconds.rem_euclid(86_400);
    let (year, month, day) = civil_from_days(days);
    format!(
        "{year:04}-{month:02}-{day:02} {:02}:{:02}:{:02} UTC",
        rest / 3600,
        rest % 3600 / 60,
        rest % 60
    )
}

fn days_from_civil(year: i64, month: i64, day: i64) -> i64 {
    let year = if month <= 2 { year - 1 } else { year };
    let era = if year >= 0 { year } else { year - 399 } / 400;
    let year_of_era = year - era * 400;
    let month_pos = if month > 2 { month - 3 } else { month + 9 };
    let day_of_year = (153 * month_pos + 2) / 5 + day - 1;
    let day_of_era = year_of_era * 365 + year_of_era / 4 - year_of_era / 100 + day_of_year;
    era * 146_097 + day_of_era - 719_468
}

fn civil_from_days(days: i64) -> (i64, u32, u32) {
    let shifted = days + 719_468;
    let era = if shifted >= 0 {
        shifted
    } else {
        shifted - 146_096
    } / 146_097;
    let day_of_era = (shifted - era * 146_097) as u64;
    let year_of_era =
        (day_of_era - day_of_era / 1_460 + day_of_era / 36_524 - day_of_era / 146_096) / 365;
    let year = year_of_era as i64 + era * 400;
    let day_of_year = day_of_era - (365 * year_of_era + year_of_era / 4 - year_of_era / 100);
    let month_pos = (5 * day_of_year + 2) / 153;
    let day = (day_of_year - (153 * month_pos + 2) / 5 + 1) as u32;
    let month = if month_pos < 10 {
        month_pos + 3
    } else {
        month_pos - 9
    } as u32;
    (if month <= 2 { year + 1 } else { year }, month, day)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_timestamps() {
        assert_eq!(parse_stamp("1970-01-01T00:00:00Z"), Some(0));
        assert_eq!(parse_stamp("2023-11-14T22:13:20Z"), Some(1_700_000_000));
        assert_eq!(parse_stamp("bad"), None);
    }

    #[test]
    fn formats_timestamps() {
        assert_eq!(format_epoch(0), "1970-01-01 00:00:00 UTC");
        assert_eq!(format_epoch(1_700_000_000), "2023-11-14 22:13:20 UTC");
    }

    #[test]
    fn roundtrips() {
        for stamp in ["2000-02-29T23:59:59Z", "1999-12-31T00:00:01Z"] {
            let epoch = parse_stamp(stamp).unwrap();
            let text = format_epoch(epoch);
            assert_eq!(
                parse_stamp(&text.replace(' ', "T").replace(" UTC", "Z")),
                Some(epoch)
            );
        }
    }
}
