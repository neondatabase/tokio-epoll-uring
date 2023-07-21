mod borrow_based;
pub use borrow_based::SystemLifecycle as BorrowBased;

mod thread_local;
pub use thread_local::SystemLifecycle as ThreadLocal;
