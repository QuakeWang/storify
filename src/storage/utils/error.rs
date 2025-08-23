// Error conversion helpers and wrapping macro for Snafu-based errors
use crate::error::Error;

/// Convert different error types into our unified Error type.
pub trait IntoStorifyError {
    fn into_error(self) -> Error;
}

impl IntoStorifyError for Error {
    fn into_error(self) -> Error {
        self
    }
}

impl IntoStorifyError for opendal::Error {
    fn into_error(self) -> Error {
        self.into()
    }
}

impl IntoStorifyError for std::io::Error {
    fn into_error(self) -> Error {
        self.into()
    }
}

/// Macro to wrap a Result-producing expression into a Snafu variant with `source: Box<Error>`.
/// Example:
/// wrap_err!(op.await, DownloadFailed { remote_path: rp, local_path: lp })?
#[macro_export]
macro_rules! wrap_err {
    ($expr:expr, $variant:ident { $($field:ident : $value:expr),* $(,)? }) => {{
        $expr.map_err(|e| {
            let src: $crate::error::Error = $crate::storage::utils::error::IntoStorifyError::into_error(e);
            $crate::error::Error::$variant { $($field: $value),*, source: Box::new(src) }
        })
    }};
}
