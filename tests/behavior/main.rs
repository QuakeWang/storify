use libtest_mimic::Arguments;
use libtest_mimic::Trial;
use storify::error::Result;

mod operations;
mod utils;

pub use utils::*;

fn main() -> Result<()> {
    let args = Arguments::from_args();

    let client = TEST_RUNTIME.block_on(init_test_service())?;

    let mut tests = Vec::new();

    operations::list::tests(&client, &mut tests);
    operations::copy::tests(&client, &mut tests);
    operations::delete::tests(&client, &mut tests);
    operations::download::tests(&client, &mut tests);
    operations::mkdir::tests(&client, &mut tests);
    operations::mv::tests(&client, &mut tests);
    operations::upload::tests(&client, &mut tests);
    operations::cat::tests(&client, &mut tests);
    operations::usage::tests(&client, &mut tests);
    operations::stat::tests(&client, &mut tests);

    let _ = tracing_subscriber::fmt()
        .pretty()
        .with_test_writer()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .try_init();

    let conclusion = libtest_mimic::run(&args, tests);

    TEST_RUNTIME.block_on(TEST_FIXTURE.cleanup(client.operator()));

    conclusion.exit()
}
