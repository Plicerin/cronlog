//! When to colour terminal output, and which colour each run status gets.

use clap::ValueEnum;
use comfy_table::Color;
use std::io::IsTerminal;

/// The `--color` setting.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, ValueEnum)]
pub enum ColorChoice {
    /// Colour only when printing to a terminal, unless NO_COLOR is set or TERM is dumb
    #[default]
    Auto,
    /// Always colour, even when piped
    Always,
    /// Never colour
    Never,
}

/// Decides from the setting and the environment. Split out so it can be tested.
fn resolve(
    choice: ColorChoice,
    is_terminal: bool,
    no_color: Option<&str>,
    term: Option<&str>,
) -> bool {
    match choice {
        ColorChoice::Always => true,
        ColorChoice::Never => false,
        ColorChoice::Auto => {
            // https://no-color.org: any non-empty value turns colour off.
            let no_color = no_color.is_some_and(|v| !v.is_empty());
            is_terminal && !no_color && term != Some("dumb")
        }
    }
}

/// Whether to colour what this process prints to stdout.
pub fn for_stdout(choice: ColorChoice) -> bool {
    let no_color = std::env::var("NO_COLOR").ok();
    let term = std::env::var("TERM").ok();
    let wanted = resolve(
        choice,
        std::io::stdout().is_terminal(),
        no_color.as_deref(),
        term.as_deref(),
    );
    // `always` keeps colour even if the console can't be switched to ANSI mode.
    wanted && (enable_ansi() || choice == ColorChoice::Always)
}

/// Older Windows consoles need ANSI processing switched on first.
#[cfg(windows)]
fn enable_ansi() -> bool {
    crossterm::ansi_support::supports_ansi()
}

#[cfg(not(windows))]
fn enable_ansi() -> bool {
    true
}

/// The colour for a run status. Standard terminal colours, so light and dark
/// themes both keep them readable.
pub fn for_status(status: &str) -> Option<Color> {
    match status {
        "success" => Some(Color::DarkGreen),
        "failed" | "timed_out" | "interrupted" => Some(Color::DarkRed),
        "skipped" => Some(Color::DarkYellow),
        "running" => Some(Color::DarkCyan),
        _ => None,
    }
}

/// Wraps text in an ANSI colour, for output that isn't a table.
pub fn paint(text: &str, ansi: &str, on: bool) -> String {
    if on {
        format!("\x1b[{ansi}m{text}\x1b[0m")
    } else {
        text.to_string()
    }
}

pub const GREEN: &str = "32";
pub const RED: &str = "31";

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn auto_colours_only_a_terminal() {
        assert!(resolve(ColorChoice::Auto, true, None, Some("xterm")));
        assert!(!resolve(ColorChoice::Auto, false, None, Some("xterm")));
    }

    #[test]
    fn auto_respects_no_color_and_dumb_terminals() {
        assert!(!resolve(ColorChoice::Auto, true, Some("1"), None));
        // An empty NO_COLOR doesn't count.
        assert!(resolve(ColorChoice::Auto, true, Some(""), None));
        assert!(!resolve(ColorChoice::Auto, true, None, Some("dumb")));
    }

    #[test]
    fn always_and_never_ignore_the_environment() {
        assert!(resolve(ColorChoice::Always, false, Some("1"), Some("dumb")));
        assert!(!resolve(ColorChoice::Never, true, None, Some("xterm")));
    }

    #[test]
    fn every_status_has_a_colour() {
        for status in [
            "success",
            "failed",
            "timed_out",
            "interrupted",
            "skipped",
            "running",
        ] {
            assert!(for_status(status).is_some(), "{status}");
        }
        assert_eq!(for_status("success"), Some(Color::DarkGreen));
        assert_eq!(for_status("failed"), Some(Color::DarkRed));
        assert_eq!(for_status("nonsense"), None);
    }

    #[test]
    fn paint_adds_codes_only_when_on() {
        assert_eq!(paint("ok", GREEN, false), "ok");
        assert_eq!(paint("ok", GREEN, true), "\x1b[32mok\x1b[0m");
    }
}
