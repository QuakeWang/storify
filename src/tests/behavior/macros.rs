macro_rules! register_behavior_tests {
    ($($test:ident),+ $(,)?) => {
        pub fn tests(client: &StorageClient, tests: &mut Vec<Trial>) {
            tests.extend(async_trials!(client, $( $test ),+));
        }

        #[cfg(test)]
        mod case_tests {
            $(
            #[test]
            fn $test() {
                if let Err(err) = crate::tests::behavior::run_behavior_case(
                    concat!("behavior::", stringify!($test)),
                ) {
                    panic!("behavior case {} failed: {err}", stringify!($test));
                }
            }
            )*
        }
    };
}
