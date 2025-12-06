use crate::error::{Error, Result};
use libtest_mimic::Arguments;
pub use libtest_mimic::Trial;
use std::env;
use std::sync::{LazyLock, Mutex};

#[macro_use]
mod macros;

pub mod operations;
pub mod utils;

pub use utils::*;

pub const SKIP_ENV: &str = "STORIFY_SKIP_BEHAVIOR";

pub fn run_behavior_suite() -> Result<()> {
    if behavior_skipped("behavior suite") {
        return Ok(());
    }
    let _guard = BEHAVIOR_MUTEX.lock().unwrap();
    run_behavior_with_args(behavior_arguments(None))
}

pub fn run_behavior_case(name: &str) -> Result<()> {
    if behavior_skipped(name) {
        return Ok(());
    }
    let _guard = BEHAVIOR_MUTEX.lock().unwrap();
    run_behavior_with_args(behavior_arguments(Some(name)))
}

fn run_behavior_with_args(args: Arguments) -> Result<()> {
    let client = TEST_RUNTIME.block_on(init_test_service())?;

    let mut tests = Vec::new();

    operations::list::tests(&client, &mut tests);
    operations::copy::tests(&client, &mut tests);
    operations::delete::tests(&client, &mut tests);
    operations::download::tests(&client, &mut tests);
    operations::head::tests(&client, &mut tests);
    operations::grep::tests(&client, &mut tests);
    operations::find::tests(&client, &mut tests);
    operations::tail::tests(&client, &mut tests);
    operations::mkdir::tests(&client, &mut tests);
    operations::mv::tests(&client, &mut tests);
    operations::upload::tests(&client, &mut tests);
    operations::cat::tests(&client, &mut tests);
    operations::usage::tests(&client, &mut tests);
    operations::stat::tests(&client, &mut tests);
    operations::tree::tests(&client, &mut tests);
    operations::diff::tests(&client, &mut tests);
    operations::touch::tests(&client, &mut tests);
    operations::append::tests(&client, &mut tests);

    let _ = tracing_subscriber::fmt()
        .pretty()
        .with_test_writer()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .try_init();

    let conclusion = libtest_mimic::run(&args, tests);

    TEST_RUNTIME.block_on(TEST_FIXTURE.cleanup(client.operator()));

    if conclusion.has_failed() {
        return Err(Error::InvalidArgument {
            message: "storify behavior tests failed".to_string(),
        });
    }

    Ok(())
}

fn behavior_arguments(filter: Option<&str>) -> Arguments {
    let mut args = base_behavior_arguments();
    if let Some(filter) = filter {
        args.filter = Some(filter.to_string());
        args.exact = true;
    }
    args
}

fn base_behavior_arguments() -> Arguments {
    let mut raw = env::args();
    let mut filtered = Vec::new();
    if let Some(bin) = raw.next() {
        filtered.push(bin);
    }

    let mut filter_removed = false;
    let mut saw_double_dash = false;

    for arg in raw {
        if !saw_double_dash && arg == "--" {
            saw_double_dash = true;
            filtered.push(arg);
            continue;
        }

        if !saw_double_dash && !filter_removed && !arg.starts_with('-') && is_harness_filter(&arg) {
            filter_removed = true;
            continue;
        }

        filtered.push(arg);
    }

    Arguments::from_iter(filtered)
}

fn is_harness_filter(arg: &str) -> bool {
    arg == "behavior_suite" || arg == "tests::behavior_suite" || arg.contains("case_tests::")
}

static BEHAVIOR_MUTEX: LazyLock<Mutex<()>> = LazyLock::new(|| Mutex::new(()));

fn behavior_skipped(label: &str) -> bool {
    if env::var(SKIP_ENV).is_ok() {
        eprintln!("behavior test '{label}' skipped because {SKIP_ENV} is set");
        true
    } else {
        false
    }
}
