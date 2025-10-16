use crate::error::Result;
use crate::storage::utils::error::IntoStorifyError;
use crate::wrap_err;
use futures::stream::TryStreamExt;
use opendal::Operator;
use std::collections::{BTreeMap, BTreeSet, VecDeque};

/// Trait for printing a directory tree in object storage.
pub trait Treer {
    /// Print the directory structure as a tree.
    ///
    /// - `path`: root path to show
    /// - `max_depth`: Some(n) to limit depth; None or Some(0) means unlimited
    /// - `dirs_only`: show directories only
    async fn tree(&self, path: &str, max_depth: Option<usize>, dirs_only: bool) -> Result<()>;
}

pub struct OpenDalTreer {
    operator: Operator,
}

impl OpenDalTreer {
    pub fn new(operator: Operator) -> Self {
        Self { operator }
    }

    fn print_branch(prefix_flags: &[bool], name: &str, is_dir: bool, is_last: bool) {
        let mut line = String::with_capacity(prefix_flags.len() * 4 + 4 + name.len() + 1);
        for more in prefix_flags {
            if *more {
                line.push_str("│   ");
            } else {
                line.push_str("    ");
            }
        }
        line.push_str(if is_last { "└── " } else { "├── " });
        line.push_str(name);
        if is_dir {
            line.push('/');
        }
        println!("{}", line);
    }
}

impl Treer for OpenDalTreer {
    async fn tree(&self, path: &str, max_depth: Option<usize>, dirs_only: bool) -> Result<()> {
        // Build children map directly during traversal
        let mut dir_children: BTreeMap<String, (BTreeSet<String>, BTreeSet<String>)> =
            BTreeMap::new();

        // Ensure root path normalized without leading './'
        let root = path.trim_start_matches("./");
        let root_prefix = if root.ends_with('/') {
            root.to_string()
        } else {
            format!("{}/", root)
        };

        // Helper to register directory chain like "a/b" so that "" -> a, "a" -> b
        fn add_dir_chain(
            dir_children: &mut BTreeMap<String, (BTreeSet<String>, BTreeSet<String>)>,
            chain: &str,
        ) {
            if chain.is_empty() {
                return;
            }
            let mut parent_path: String = String::new();
            for seg in chain.split('/') {
                let (dirs_set, _files_set) = dir_children.entry(parent_path.clone()).or_default();
                dirs_set.insert(seg.to_string());
                if parent_path.is_empty() {
                    parent_path.push_str(seg);
                } else {
                    parent_path.push('/');
                    parent_path.push_str(seg);
                }
            }
        }

        // Determine traversal strategy based on depth limit
        let traversal_limit = match max_depth {
            Some(0) | None => usize::MAX,
            Some(n) => n,
        };

        if traversal_limit == usize::MAX {
            // Unlimited depth: keep using a single recursive lister (fast path)
            let lister = wrap_err!(
                self.operator.lister_with(path).recursive(true).await,
                ListDirectoryFailed {
                    path: path.to_string()
                }
            )?;

            let mut stream = lister.map_err(|e| crate::error::Error::ListDirectoryFailed {
                path: path.to_string(),
                source: Box::new(e.into_error()),
            });

            while let Some(entry) = stream.try_next().await? {
                let p = entry.path();
                let rel = if p.starts_with(&root_prefix) {
                    &p[root_prefix.len()..]
                } else if p == root {
                    ""
                } else {
                    p
                };

                if rel.is_empty() {
                    continue;
                }

                let meta = entry.metadata();
                if meta.mode().is_dir() {
                    // normalize directory path without leading/trailing slashes
                    let d = rel.trim_matches('/').to_string();
                    add_dir_chain(&mut dir_children, &d);
                } else if !dirs_only {
                    // treat file only when dirs_only is false
                    if let Some((parent, name)) = rel.rsplit_once('/') {
                        let parent_norm = parent.trim_matches('/');
                        add_dir_chain(&mut dir_children, parent_norm);
                        let (_ds, fs) = dir_children.entry(parent_norm.to_string()).or_default();
                        fs.insert(name.to_string());
                    } else {
                        let (_ds, fs) = dir_children.entry(String::new()).or_default();
                        fs.insert(rel.to_string());
                    }
                }
            }
        } else {
            // Limited depth: BFS by levels, only list down to traversal_limit
            let mut queue: VecDeque<(String, usize)> = VecDeque::new();
            // (abs_dir, depth)
            queue.push_back((root.to_string(), 0));

            while let Some((abs_dir, depth)) = queue.pop_front() {
                let lister = wrap_err!(
                    self.operator.lister_with(&abs_dir).recursive(false).await,
                    ListDirectoryFailed {
                        path: abs_dir.clone()
                    }
                )?;

                let mut stream = lister.map_err(|e| crate::error::Error::ListDirectoryFailed {
                    path: abs_dir.clone(),
                    source: Box::new(e.into_error()),
                });

                while let Some(entry) = stream.try_next().await? {
                    let p = entry.path();
                    let rel = if p.starts_with(&root_prefix) {
                        &p[root_prefix.len()..]
                    } else if p == root {
                        ""
                    } else {
                        p
                    };

                    if rel.is_empty() {
                        continue;
                    }

                    let meta = entry.metadata();
                    if meta.mode().is_dir() {
                        let d = rel.trim_matches('/').to_string();
                        add_dir_chain(&mut dir_children, &d);
                        // Only traverse deeper if within depth limit
                        if depth + 1 < traversal_limit {
                            // entry.path() should be the absolute directory path
                            queue.push_back((p.to_string(), depth + 1));
                        }
                    } else if !dirs_only {
                        if let Some((parent, name)) = rel.rsplit_once('/') {
                            let parent_norm = parent.trim_matches('/');
                            add_dir_chain(&mut dir_children, parent_norm);
                            let (_ds, fs) =
                                dir_children.entry(parent_norm.to_string()).or_default();
                            fs.insert(name.to_string());
                        } else {
                            let (_ds, fs) = dir_children.entry(String::new()).or_default();
                            fs.insert(rel.to_string());
                        }
                    }
                }
            }
        }

        // DFS print with Unicode connectors
        let limit = match max_depth {
            Some(0) | None => usize::MAX,
            Some(n) => n,
        };
        // print root label and ensure directories end with a trailing slash
        let root_label: String = if root.is_empty() || root == "/" {
            "/".to_string()
        } else {
            format!("{}/", root.trim_end_matches('/'))
        };
        println!("{}", root_label);

        fn dfs(
            cwd: &str,
            prefix_flags: &mut Vec<bool>,
            limit: usize,
            dirs_only: bool,
            dir_children: &BTreeMap<String, (BTreeSet<String>, BTreeSet<String>)>,
        ) {
            // depth equals current prefix length; stop if reached limit
            if prefix_flags.len() >= limit {
                return;
            }

            if let Some((dir_set, file_set)) = dir_children.get(cwd) {
                // build ordered children list: dirs then files
                let mut items: Vec<(String, bool)> = Vec::new();
                for d in dir_set.iter() {
                    items.push((d.clone(), true));
                }
                if !dirs_only {
                    for f in file_set.iter() {
                        items.push((f.clone(), false));
                    }
                }

                let total = items.len();
                for (idx, (name, is_dir)) in items.into_iter().enumerate() {
                    let is_last = idx + 1 == total;
                    OpenDalTreer::print_branch(prefix_flags, &name, is_dir, is_last);
                    if is_dir {
                        let next = if cwd.is_empty() {
                            name.clone()
                        } else {
                            format!("{}/{}", cwd, name)
                        };
                        // push whether there are more siblings at this level to draw vertical bars
                        prefix_flags.push(!is_last);
                        dfs(&next, prefix_flags, limit, dirs_only, dir_children);
                        prefix_flags.pop();
                    }
                }
            }
        }

        let mut prefix_flags: Vec<bool> = Vec::new();
        dfs("", &mut prefix_flags, limit, dirs_only, &dir_children);
        Ok(())
    }
}
