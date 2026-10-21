//! Stops a bench whose set-up fails, since a failed set-up measures nothing.

pub trait Must<T> {
    fn must(self, what: &str) -> T;
}

impl<T, E: std::fmt::Debug> Must<T> for Result<T, E> {
    fn must(self, what: &str) -> T {
        match self {
            Ok(value) => value,
            Err(error) => {
                eprintln!("{what}: {error:?}");
                std::process::exit(1)
            }
        }
    }
}

impl<T> Must<T> for Option<T> {
    fn must(self, what: &str) -> T {
        match self {
            Some(value) => value,
            None => {
                eprintln!("{what}");
                std::process::exit(1)
            }
        }
    }
}
