use std::{io::Write, os::unix::process::CommandExt};

/// Check whether a [`crate::System`]'s consumes memlock rlimit budget.
///
/// Unsafe because this function forks & uses [`std::os::unix::process::Command::pre_exec`].
///
/// Panics in a bunch of places.
///
pub unsafe fn does_system_consume_memlock_rlimit_budget() -> bool {
    // Fork a child process.
    // In the child process, set up a tokio-epoll-uring system.
    // Compare the memlock rlimit usage before and after.

    // poor man's bincode
    #[repr(C, packed)]
    struct RlimitPrePost {
        pre: (u64, u64),
        post: (u64, u64),
    }
    // Ensure `true` binary exists.
    std::process::Command::new("true")
        .output()
        .expect("canont exec `true` binary");
    let output = std::process::Command::new("true")
        .pre_exec(|| {
            let rt = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()?;
            let pre = nix::sys::resource::getrlimit(nix::sys::resource::Resource::RLIMIT_MEMLOCK)
                .unwrap();
            let system = rt.block_on(crate::System::launch()).unwrap();
            // use the memory
            let ((), res) = rt.block_on(system.nop());
            res.unwrap();
            let post = nix::sys::resource::getrlimit(nix::sys::resource::Resource::RLIMIT_MEMLOCK)
                .unwrap();
            let output = RlimitPrePost { pre, post };
            let output_as_slice = std::slice::from_raw_parts(
                &output as *const _ as *const u8,
                std::mem::size_of_val(&output),
            );
            std::io::stdout().write_all(output_as_slice).unwrap();
            Ok(())
        })
        .output()
        .unwrap();
    assert_eq!(output.status.code().unwrap(), 0);
    assert_eq!(output.stdout.len(), std::mem::size_of::<RlimitPrePost>());
    let (head, output, tail): (_, &[RlimitPrePost], _) = output.stdout.align_to();
    assert_eq!(head.len(), 0);
    assert_eq!(tail.len(), 0);
    assert_eq!(output.len(), 1);
    let RlimitPrePost { pre, post } = output[0];
    assert!(pre <= post, "above code can't use _less_ locked memory");
    pre < post
}
