use nix::{fcntl::FallocateFlags, libc::O_DIRECT};
use std::os::unix::fs::OpenOptionsExt;

#[tokio::main]
async fn main() {
    let fallocate: Option<FallocateFlags> = {
        let arg = std::env::args().nth(1).unwrap_or_else(|| {
            panic!("missing first argument, must be one of fallocate-keep-size, fallocate-0, no-fallocate")
        });
        match arg.as_str() {
            "fallocate-keep-size" => Some(FallocateFlags::FALLOC_FL_KEEP_SIZE),
            "fallocate-0" => Some(FallocateFlags::empty()),
            "no-fallocate" => None,
            _ => panic!("invalid argument"),
        }
    };

    let system = tokio_epoll_uring::System::launch().await.unwrap();

    let file = "testfile.data";
    match std::fs::remove_file(file) {
        Ok(_) => println!("File removed successfully"),
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
            println!("File not found, proceeding to create a new one");
        }
        Err(e) => {
            panic!("{e}");
        }
    }

    println!("creating file");
    let file = std::fs::OpenOptions::new()
        .write(true)
        .create_new(true)
        .custom_flags(O_DIRECT)
        .open(file)
        .unwrap();

    if let Some(flags) = fallocate {
        println!("fallocating");
        use std::os::fd::AsRawFd;
        nix::fcntl::fallocate(file.as_raw_fd(), flags, 0, 8192).unwrap();
    } else {
        println!("skipping fallocate");
    }

    println!("issuing the write");
    let fd: std::os::fd::OwnedFd = file.into();
    let buf =
        unsafe { std::alloc::alloc(std::alloc::Layout::from_size_align(8192, 8192).unwrap()) };
    if buf.is_null() {
        panic!("Failed to allocate buffer");
    }
    let mut vec = unsafe { Vec::from_raw_parts(buf, 8192, 8192) };
    vec.fill(1);

    let (_, res) = system.write(fd, 0, vec).await;
    let written = res.unwrap();
    assert_eq!(written, 8192, "not expecting short write");
}
