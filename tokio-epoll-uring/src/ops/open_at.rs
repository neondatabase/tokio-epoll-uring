/// NB: a good chunk of this file has been copied from the `tokio_uring` crate
/// and most of it is unused. Would be much nicer to reuse tokio-uring's OpenOptions,
/// maybe by forking it and making the conversion to libc flags pub?

use crate::system::submission::op_fut::Op;
use std::ffi::CString;
use std::os::fd::{AsRawFd, FromRawFd, OwnedFd};
use std::os::unix::prelude::OsStrExt;
use std::path::Path;

pub use self::open_options::OpenOptions;

pub struct OpenAtOp {
    dir_fd: Option<OwnedFd>,
    pathname: CString,
    flags: libc::c_int,
    mode: libc::mode_t,
}

impl OpenAtOp {
    pub(crate) fn new(
        dir_fd: Option<OwnedFd>,
        path: &Path,
        options: &OpenOptions,
    ) -> std::io::Result<Self> {
        let pathname = CString::new(path.as_os_str().as_bytes())?;

        let flags = libc::O_CLOEXEC
            | options.access_mode()?
            | options.creation_mode()?
            | (options.custom_flags & !libc::O_ACCMODE);

        Ok(OpenAtOp {
            dir_fd,
            pathname,
            flags,
            mode: options.mode,
        })
    }
}

impl crate::sealed::Sealed for OpenAtOp {}

impl Op for OpenAtOp {
    type Resources = Option<OwnedFd>;
    type Success = OwnedFd;
    type Error = std::io::Error;

    fn make_sqe(&mut self) -> io_uring::squeue::Entry {
        io_uring::opcode::OpenAt::new(
            io_uring::types::Fd(
                self.dir_fd
                    .as_ref()
                    .map(|fd| fd.as_raw_fd())
                    .unwrap_or(libc::AT_FDCWD),
            ),
            self.pathname.as_ptr(),
        )
        .flags(self.flags)
        .mode(self.mode)
        .build()
    }

    fn on_failed_submission(self) -> Self::Resources {
        self.dir_fd
    }

    fn on_op_completion(
        self,
        res: i32,
    ) -> (Self::Resources, Result<Self::Success, Self::Error>) {
        // https://man.archlinux.org/man/io_uring_prep_openat.3.en
        // and https://man.archlinux.org/man/openat.2.en
        let res = if res < 0 {
            Err(std::io::Error::from_raw_os_error(-res))
        } else {
            Ok(unsafe { OwnedFd::from_raw_fd(res) })
        };
        (self.dir_fd, res)
    }
}

/// Copied from `tokio_uring` crate.
pub mod open_options {
    use std::io;
    
    use std::os::unix::fs::OpenOptionsExt;
    

    

    /// Options and flags which can be used to configure how a file is opened.
    ///
    /// This builder exposes the ability to configure how a [`File`] is opened and
    /// what operations are permitted on the open file. The [`File::open`] and
    /// [`File::create`] methods are aliases for commonly used options using this
    /// builder.
    ///
    /// Generally speaking, when using `OpenOptions`, you'll first call
    /// [`OpenOptions::new`], then chain calls to methods to set each option, then
    /// call [`OpenOptions::open`], passing the path of the file you're trying to
    /// open. This will give you a [`io::Result`] with a [`File`] inside that you
    /// can further operate on.
    ///
    /// # Examples
    ///
    /// Opening a file to read:
    ///
    /// ```no_run
    /// use tokio_uring::fs::OpenOptions;
    ///
    /// fn main() -> Result<(), Box<dyn std::error::Error>> {
    ///     tokio_uring::start(async {
    ///         let file = OpenOptions::new()
    ///             .read(true)
    ///             .open("foo.txt")
    ///             .await?;
    ///         Ok(())
    ///     })
    /// }
    /// ```
    ///
    /// Opening a file for both reading and writing, as well as creating it if it
    /// doesn't exist:
    ///
    /// ```no_run
    /// use tokio_uring::fs::OpenOptions;
    ///
    /// fn main() -> Result<(), Box<dyn std::error::Error>> {
    ///     tokio_uring::start(async {
    ///         let file = OpenOptions::new()
    ///             .read(true)
    ///             .write(true)
    ///             .create(true)
    ///             .open("foo.txt")
    ///             .await?;
    ///         Ok(())
    ///     })
    /// }
    /// ```
    #[derive(Debug, Clone)]
    pub struct OpenOptions {
        read: bool,
        write: bool,
        append: bool,
        truncate: bool,
        create: bool,
        create_new: bool,
        pub(crate) mode: libc::mode_t,
        pub(crate) custom_flags: libc::c_int,
    }

    impl OpenOptions {
        /// Creates a blank new set of options ready for configuration.
        ///
        /// All options are initially set to `false`.
        ///
        /// # Examples
        ///
        /// ```no_run
        /// use tokio_uring::fs::OpenOptions;
        ///
        /// fn main() -> Result<(), Box<dyn std::error::Error>> {
        ///     tokio_uring::start(async {
        ///         let file = OpenOptions::new()
        ///             .read(true)
        ///             .open("foo.txt")
        ///             .await?;
        ///         Ok(())
        ///     })
        /// }
        /// ```
        pub fn new() -> OpenOptions {
            OpenOptions {
                // generic
                read: false,
                write: false,
                append: false,
                truncate: false,
                create: false,
                create_new: false,
                mode: 0o666,
                custom_flags: 0,
            }
        }

        /// Sets the option for read access.
        ///
        /// This option, when true, will indicate that the file should be
        /// `read`-able if opened.
        ///
        /// # Examples
        ///
        /// ```no_run
        /// use tokio_uring::fs::OpenOptions;
        ///
        /// fn main() -> Result<(), Box<dyn std::error::Error>> {
        ///     tokio_uring::start(async {
        ///         let file = OpenOptions::new()
        ///             .read(true)
        ///             .open("foo.txt")
        ///             .await?;
        ///         Ok(())
        ///     })
        /// }
        /// ```
        pub fn read(&mut self, read: bool) -> &mut OpenOptions {
            self.read = read;
            self
        }

        /// Sets the option for write access.
        ///
        /// This option, when true, will indicate that the file should be
        /// `write`-able if opened.
        ///
        /// If the file already exists, any write calls on it will overwrite its
        /// contents, without truncating it.
        ///
        /// # Examples
        ///
        /// ```no_run
        /// use tokio_uring::fs::OpenOptions;
        ///
        /// fn main() -> Result<(), Box<dyn std::error::Error>> {
        ///     tokio_uring::start(async {
        ///         let file = OpenOptions::new()
        ///             .write(true)
        ///             .open("foo.txt")
        ///             .await?;
        ///         Ok(())
        ///     })
        /// }
        /// ```
        pub fn write(&mut self, write: bool) -> &mut OpenOptions {
            self.write = write;
            self
        }

        /// Sets the option for the append mode.
        ///
        /// This option, when true, means that writes will append to a file instead
        /// of overwriting previous contents. Note that setting
        /// `.write(true).append(true)` has the same effect as setting only
        /// `.append(true)`.
        ///
        /// For most filesystems, the operating system guarantees that all writes
        /// are atomic: no writes get mangled because another process writes at the
        /// same time.
        ///
        /// ## Note
        ///
        /// This function doesn't create the file if it doesn't exist. Use the
        /// [`OpenOptions::create`] method to do so.
        ///
        /// # Examples
        ///
        /// ```no_run
        /// use tokio_uring::fs::OpenOptions;
        ///
        /// fn main() -> Result<(), Box<dyn std::error::Error>> {
        ///     tokio_uring::start(async {
        ///         let file = OpenOptions::new()
        ///             .append(true)
        ///             .open("foo.txt")
        ///             .await?;
        ///         Ok(())
        ///     })
        /// }
        /// ```
        pub fn append(&mut self, append: bool) -> &mut OpenOptions {
            self.append = append;
            self
        }

        /// Sets the option for truncating a previous file.
        ///
        /// If a file is successfully opened with this option set it will truncate
        /// the file to 0 length if it already exists.
        ///
        /// The file must be opened with write access for truncate to work.
        ///
        /// # Examples
        ///
        /// ```no_run
        /// use tokio_uring::fs::OpenOptions;
        ///
        /// fn main() -> Result<(), Box<dyn std::error::Error>> {
        ///     tokio_uring::start(async {
        ///         let file = OpenOptions::new()
        ///             .write(true)
        ///             .truncate(true)
        ///             .open("foo.txt")
        ///             .await?;
        ///         Ok(())
        ///     })
        /// }
        /// ```
        pub fn truncate(&mut self, truncate: bool) -> &mut OpenOptions {
            self.truncate = truncate;
            self
        }

        /// Sets the option to create a new file, or open it if it already exists.
        ///
        /// In order for the file to be created, [`OpenOptions::write`] or
        /// [`OpenOptions::append`] access must be used.
        ///
        /// # Examples
        ///
        /// ```no_run
        /// use tokio_uring::fs::OpenOptions;
        ///
        /// fn main() -> Result<(), Box<dyn std::error::Error>> {
        ///     tokio_uring::start(async {
        ///         let file = OpenOptions::new()
        ///             .write(true)
        ///             .create(true)
        ///             .open("foo.txt")
        ///             .await?;
        ///         Ok(())
        ///     })
        /// }
        /// ```
        pub fn create(&mut self, create: bool) -> &mut OpenOptions {
            self.create = create;
            self
        }

        /// Sets the option to create a new file, failing if it already exists.
        ///
        /// No file is allowed to exist at the target location, also no (dangling) symlink. In this
        /// way, if the call succeeds, the file returned is guaranteed to be new.
        ///
        /// This option is useful because it is atomic. Otherwise between checking
        /// whether a file exists and creating a new one, the file may have been
        /// created by another process (a TOCTOU race condition / attack).
        ///
        /// If `.create_new(true)` is set, [`.create()`] and [`.truncate()`] are
        /// ignored.
        ///
        /// The file must be opened with write or append access in order to create
        /// a new file.
        ///
        /// [`.create()`]: OpenOptions::create
        /// [`.truncate()`]: OpenOptions::truncate
        ///
        /// # Examples
        ///
        /// ```no_run
        /// use tokio_uring::fs::OpenOptions;
        ///
        /// fn main() -> Result<(), Box<dyn std::error::Error>> {
        ///     tokio_uring::start(async {
        ///         let file = OpenOptions::new()
        ///             .write(true)
        ///             .create_new(true)
        ///             .open("foo.txt")
        ///             .await?;
        ///         Ok(())
        ///     })
        /// }
        /// ```
        pub fn create_new(&mut self, create_new: bool) -> &mut OpenOptions {
            self.create_new = create_new;
            self
        }

        pub(crate) fn access_mode(&self) -> io::Result<libc::c_int> {
            match (self.read, self.write, self.append) {
                (true, false, false) => Ok(libc::O_RDONLY),
                (false, true, false) => Ok(libc::O_WRONLY),
                (true, true, false) => Ok(libc::O_RDWR),
                (false, _, true) => Ok(libc::O_WRONLY | libc::O_APPEND),
                (true, _, true) => Ok(libc::O_RDWR | libc::O_APPEND),
                (false, false, false) => Err(io::Error::from_raw_os_error(libc::EINVAL)),
            }
        }

        pub(crate) fn creation_mode(&self) -> io::Result<libc::c_int> {
            match (self.write, self.append) {
                (true, false) => {}
                (false, false) => {
                    if self.truncate || self.create || self.create_new {
                        return Err(io::Error::from_raw_os_error(libc::EINVAL));
                    }
                }
                (_, true) => {
                    if self.truncate && !self.create_new {
                        return Err(io::Error::from_raw_os_error(libc::EINVAL));
                    }
                }
            }

            Ok(match (self.create, self.truncate, self.create_new) {
                (false, false, false) => 0,
                (true, false, false) => libc::O_CREAT,
                (false, true, false) => libc::O_TRUNC,
                (true, true, false) => libc::O_CREAT | libc::O_TRUNC,
                (_, _, true) => libc::O_CREAT | libc::O_EXCL,
            })
        }
    }

    impl Default for OpenOptions {
        fn default() -> Self {
            Self::new()
        }
    }

    impl OpenOptionsExt for OpenOptions {
        fn mode(&mut self, mode: u32) -> &mut OpenOptions {
            self.mode = mode;
            self
        }

        fn custom_flags(&mut self, flags: i32) -> &mut OpenOptions {
            self.custom_flags = flags;
            self
        }
    }
}
