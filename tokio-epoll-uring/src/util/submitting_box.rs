//! See [`SubmittingBox`].

/// A wrapper around [`Box`] with an API that forces users to spell out
/// ownerhsip transitions of the memory between kernel and userspace.
pub enum SubmittingBox<T>
where
    T: 'static,
{
    NotSubmitting { inner: Box<T> },
    Submitting(*mut T),
    Undefined,
}

unsafe impl<T> Send for SubmittingBox<T> where T: Send {}

impl<T> SubmittingBox<T> {
    pub(crate) fn new(inner: Box<T>) -> Self {
        Self::NotSubmitting { inner }
    }

    /// [`Box::leak`] the inner box.
    ///
    /// # Panics
    ///
    /// Panics if this function has already been called on `self`
    /// before without a call to [`Self::ownership_back_in_userspace`] inbetween.
    pub(crate) fn start_submitting(&mut self) -> &'static mut T {
        match std::mem::replace(self, Self::Undefined) {
            SubmittingBox::NotSubmitting { inner } => {
                let leaked = Box::leak(inner);
                *self = Self::Submitting(leaked as *mut _);
                leaked
            }
            SubmittingBox::Submitting(_) => {
                panic!("must not call this function more than once without ownership_back_in_userspace() inbetween")
            }
            Self::Undefined => {
                panic!("implementation error; did we panic earlier in the ::Submitting case?")
            }
        }
    }

    /// [`Box::from_raw`] the inner box.
    ///
    /// # Panics
    ///
    /// Panics if there was no preceding call to [`Self::start_submitting`].
    ///
    /// # Safety
    ///
    /// Callers must ensure that userspace, and in particular, _the caller_ has again exclusive ownership
    /// over the memory.
    pub(crate) unsafe fn ownership_back_in_userspace(mut self) -> Box<T> {
        match std::mem::replace(&mut self, SubmittingBox::Undefined) {
            SubmittingBox::NotSubmitting { .. } => {
                panic!("must not call this function without prior call to start_submitting()")
            }
            SubmittingBox::Submitting(leaked) => Box::from_raw(leaked),
            SubmittingBox::Undefined => todo!(),
        }
    }
}
