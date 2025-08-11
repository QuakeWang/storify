use libtest_mimic::Arguments;
use libtest_mimic::Trial;
use ossify::error::Result;

mod operations;
mod utils;

pub use utils::*;

fn main() -> Result<()> {
    let args = Arguments::from_args();

    let client = if let Some(client) = TEST_RUNTIME.block_on(init_test_service())? {
        client
    } else {
        return Ok(());
    };

    let mut tests = Vec::new();

    operations::list::tests(&client, &mut tests);

    let _ = tracing_subscriber::fmt()
        .pretty()
        .with_test_writer()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .try_init();

    let conclusion = libtest_mimic::run(&args, tests);

    TEST_RUNTIME.block_on(TEST_FIXTURE.cleanup(client.operator()));

    conclusion.exit()
}
