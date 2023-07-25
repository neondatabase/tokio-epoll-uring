use crate::{ResourcesOwnedByKernel, SubmitSideProvider, SystemLauncher};

use super::OpTrait;

pub async fn nop<'a, L, P>(system_launcher: L) -> std::io::Result<()>
where
    L: SystemLauncher<P> + Unpin + Send,
    P: SubmitSideProvider + Unpin,
{
    let op = Nop;
    match op.into_fut(system_launcher).await {
        Ok(output) => output,
        Err((Nop, e)) => Err(std::io::Error::new(std::io::ErrorKind::Other, e)),
    }
}

struct Nop;

impl OpTrait for Nop {
    fn make_sqe(&mut self) -> io_uring::squeue::Entry {
        io_uring::opcode::Nop::new().build()
    }
}

impl ResourcesOwnedByKernel for Nop {
    type OpResult = std::io::Result<()>;

    fn on_op_completion(self, res: i32) -> Self::OpResult {
        // https://man.archlinux.org/man/io_uring_prep_read.3.en
        if res < 0 {
            Err(std::io::Error::from_raw_os_error(-res))
        } else {
            Ok(())
        }
    }
}
