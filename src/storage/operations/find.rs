use crate::error::{Error, Result};
use crate::storage::utils::error::IntoStorifyError;
use futures::stream::TryStreamExt;
use globset::GlobMatcher;
use opendal::{EntryMode, Operator};
use regex::Regex;

pub trait Finder {
    async fn find(&self, opts: &FindOptions) -> Result<()>;
}

pub struct OpenDalFinder {
    operator: Operator,
}

impl OpenDalFinder {
    pub fn new(operator: Operator) -> Self {
        Self { operator }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EntryTypeFilter {
    File,
    Dir,
    Other,
}

pub struct FindOptions {
    pub path: String,
    pub name_glob: Option<GlobMatcher>,
    pub regex: Option<Regex>,
    pub type_filter: Option<EntryTypeFilter>,
}

impl Finder for OpenDalFinder {
    async fn find(&self, opts: &FindOptions) -> Result<()> {
        // First, try stat to determine if it's a file; if not found, map to PathNotFound
        match self.operator.stat(&opts.path).await {
            Ok(meta) => {
                if meta.mode() == EntryMode::FILE {
                    let path = opts.path.as_str();
                    if is_match(path, &meta, opts) {
                        println!("{}", path);
                    }
                    return Ok(());
                }
            }
            Err(e) => {
                if e.kind() == opendal::ErrorKind::NotFound {
                    return Err(Error::PathNotFound {
                        path: std::path::PathBuf::from(&opts.path),
                    });
                }
                // For other errors, continue to try listing (some providers treat prefixes specially)
            }
        }

        let lister = self
            .operator
            .lister_with(&opts.path)
            .recursive(true)
            .await
            .map_err(|e| Error::FindFailed {
                path: opts.path.clone(),
                source: Box::new(IntoStorifyError::into_error(e)),
            })?;

        futures::pin_mut!(lister);
        let no_filters =
            opts.type_filter.is_none() && opts.name_glob.is_none() && opts.regex.is_none();
        while let Some(entry) = lister.try_next().await.map_err(|e| Error::FindFailed {
            path: opts.path.clone(),
            source: Box::new(IntoStorifyError::into_error(e.into_error())),
        })? {
            if no_filters {
                println!("{}", entry.path());
                continue;
            }

            let meta = entry.metadata();
            let path = entry.path();
            if is_match(path, meta, opts) {
                println!("{}", path);
            }
        }
        Ok(())
    }
}

fn is_match(path: &str, meta: &opendal::Metadata, opts: &FindOptions) -> bool {
    if let Some(tf) = opts.type_filter {
        let t = match meta.mode() {
            EntryMode::FILE => EntryTypeFilter::File,
            EntryMode::DIR => EntryTypeFilter::Dir,
            _ => EntryTypeFilter::Other,
        };
        if t != tf {
            return false;
        }
    }

    if let Some(glob) = &opts.name_glob
        && !glob.is_match(path)
    {
        return false;
    }

    if let Some(re) = &opts.regex
        && !re.is_match(path)
    {
        return false;
    }

    true
}
