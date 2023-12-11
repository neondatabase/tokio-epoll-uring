pub mod io_uring {
    use std::marker::PhantomData;

    pub struct IoUring;

    pub struct CompletionQueue<'a> {
        _marker: &'a PhantomData<()>,
    }
    pub struct SubmissionQueue<'a> {
        _marker: &'a PhantomData<()>,
    }

    pub struct Submitter<'a> {
        _marker: &'a PhantomData<()>,
    }

    impl 

    impl<'a> Submitter<'a> {
        pub fn submit(&self) -> Result<(), ()> {
            todo!()
        }
    }

    impl<'a> CompletionQueue<'a> {
        pub fn sync(&self) {
            todo!()
        }
        pub fn len(&self) -> usize {
            todo!()
        }
    }

    impl<'a> Iterator for CompletionQueue<'a> {
        type Item = cqueue::Entry;
        fn next(&mut self) -> Option<Self::Item> {
            todo!()
        }
    }

    impl<'a> SubmissionQueue<'a> {
        pub fn sync(&self) {
            todo!()
        }
        pub fn len(&self) -> usize {
            todo!()
        }
        pub fn push(&self, entry: &squeue::Entry) -> Result<(), ()> {
            todo!()
        }
    }

    pub mod squeue {
        pub use io_uring::squeue::Entry;
    }

    pub mod cqueue {
        pub use io_uring::cqueue::Entry;
    }

    pub use io_uring::opcode;

    pub use io_uring::types;
}
