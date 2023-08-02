use crate::system::submission::op_fut::Op;

pub struct Nop {}

impl crate::sealed::Sealed for Nop {}

impl Op for Nop {
    type Resources = ();
    type Success = ();
    type Error = std::io::Error;

    fn make_sqe(&mut self) -> io_uring::squeue::Entry {
        io_uring::opcode::Nop::new().build()
    }

    fn on_failed_submission(self) -> Self::Resources {}

    fn on_op_completion(self, res: i32) -> (Self::Resources, Result<Self::Success, Self::Error>) {
        // https://man.archlinux.org/man/io_uring_prep_read.3.en
        let res = if res < 0 {
            Err(std::io::Error::from_raw_os_error(-res))
        } else {
            Ok(())
        };
        ((), res)
    }
}
