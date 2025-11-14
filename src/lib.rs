use std::cell::UnsafeCell;
use std::ffi::CString;
use std::io;
use std::marker::PhantomData;
use std::mem::{self, MaybeUninit};
use std::os::fd::RawFd;
use std::os::raw::{c_char, c_int, c_void};
use std::ptr;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::thread;

const O_RDWR: c_int = 0o2;
const O_CREAT: c_int = 0o100;
const O_EXCL: c_int = 0o200;
const PROT_READ: c_int = 0x1;
const PROT_WRITE: c_int = 0x2;
const MAP_SHARED: c_int = 0x01;
const MAP_FAILED: *mut c_void = !0usize as *mut c_void;

type OffT = i64;

unsafe extern "C" {
    fn shm_open(name: *const c_char, oflag: c_int, mode: c_int) -> c_int;
    fn shm_unlink(name: *const c_char) -> c_int;
    fn ftruncate(fd: c_int, length: OffT) -> c_int;
    fn mmap(
        addr: *mut c_void,
        length: usize,
        prot: c_int,
        flags: c_int,
        fd: c_int,
        offset: OffT,
    ) -> *mut c_void;
    fn munmap(addr: *mut c_void, length: usize) -> c_int;
    fn close(fd: c_int) -> c_int;
}

/// Multi-producer multi-consumer ring buffer built on top of POSIX shared memory.
///
/// The queue stores values of type `T` that implement [`Copy`]. The underlying data is placed in a
/// shared memory region so that independent processes can communicate by sharing the same POSIX
/// shared memory name.
///
/// The implementation is based on the bounded MPMC queue by Dmitry Vyukov. Each slot contains a
/// sequence counter that allows writers and readers to coordinate without additional locks.
pub struct ShmRingBuffer<T> {
    header: *mut ShmHeader,
    slots: *mut Slot<T>,
    fd: RawFd,
    layout_size: usize,
    capacity: usize,
    mask: usize,
    _marker: PhantomData<T>,
}

#[repr(C)]
struct ShmHeader {
    item_size: usize,
    slot_size: usize,
    slots_offset: usize,
    capacity: usize,
    mask: usize,
    enqueue_pos: AtomicUsize,
    dequeue_pos: AtomicUsize,
}

#[repr(C)]
struct Slot<T> {
    sequence: AtomicUsize,
    value: UnsafeCell<MaybeUninit<T>>,
}

unsafe impl<T: Copy + Send> Send for ShmRingBuffer<T> {}
unsafe impl<T: Copy + Send> Sync for ShmRingBuffer<T> {}

unsafe impl<T: Copy + Send> Send for Slot<T> {}
unsafe impl<T: Copy + Send> Sync for Slot<T> {}

/// Error returned when a push operation fails because the buffer is full.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TryPushError {
    /// The queue currently contains `capacity` elements and cannot accept more without a consumer.
    Full,
}

/// Error returned when a pop operation fails because the buffer is empty.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TryPopError {
    /// No data is available in the queue at the moment.
    Empty,
}

impl<T: Copy> ShmRingBuffer<T> {
    /// Creates a new shared-memory ring buffer with the given name and power-of-two capacity.
    pub fn create(name: &str, capacity: usize) -> io::Result<Self> {
        if capacity == 0 || !capacity.is_power_of_two() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "capacity must be a non-zero power of two",
            ));
        }

        let c_name = to_cstring(name)?;
        let flags = O_RDWR | O_CREAT | O_EXCL;
        let mode = 0o600;
        let fd = unsafe { shm_open(c_name.as_ptr(), flags, mode) };
        if fd < 0 {
            return Err(io::Error::last_os_error());
        }

        let header_size = mem::size_of::<ShmHeader>();
        let slots_offset = align_up(header_size, mem::align_of::<Slot<T>>());
        let slot_size = mem::size_of::<Slot<T>>();
        let total_size = slots_offset
            .checked_add(capacity.checked_mul(slot_size).ok_or_else(|| {
                io::Error::new(io::ErrorKind::InvalidInput, "capacity is too large")
            })?)
            .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidInput, "layout overflow"))?;

        if unsafe { ftruncate(fd, total_size as i64) } != 0 {
            let err = io::Error::last_os_error();
            unsafe {
                close(fd);
                shm_unlink(c_name.as_ptr());
            }
            return Err(err);
        }

        let ptr = unsafe {
            mmap(
                ptr::null_mut(),
                total_size,
                PROT_READ | PROT_WRITE,
                MAP_SHARED,
                fd,
                0,
            )
        };
        if ptr == MAP_FAILED {
            let err = io::Error::last_os_error();
            unsafe {
                close(fd);
                shm_unlink(c_name.as_ptr());
            }
            return Err(err);
        }

        let header_ptr = ptr as *mut ShmHeader;
        let header = ShmHeader {
            item_size: mem::size_of::<T>(),
            slot_size,
            slots_offset,
            capacity,
            mask: capacity - 1,
            enqueue_pos: AtomicUsize::new(0),
            dequeue_pos: AtomicUsize::new(0),
        };

        unsafe {
            ptr::write(header_ptr, header);
        }

        let slots_ptr = unsafe { (ptr as *mut u8).add(slots_offset) as *mut Slot<T> };
        for i in 0..capacity {
            let slot = Slot {
                sequence: AtomicUsize::new(i),
                value: UnsafeCell::new(MaybeUninit::uninit()),
            };
            unsafe {
                ptr::write(slots_ptr.add(i), slot);
            }
        }

        Ok(Self {
            header: header_ptr,
            slots: slots_ptr,
            fd,
            layout_size: total_size,
            capacity,
            mask: capacity - 1,
            _marker: PhantomData,
        })
    }

    /// Opens an existing shared-memory ring buffer previously created with [`ShmRingBuffer::create`].
    pub fn open(name: &str) -> io::Result<Self> {
        let c_name = to_cstring(name)?;
        let fd = unsafe { shm_open(c_name.as_ptr(), O_RDWR, 0) };
        if fd < 0 {
            return Err(io::Error::last_os_error());
        }

        let header_size = mem::size_of::<ShmHeader>();
        let header_map = unsafe {
            mmap(
                ptr::null_mut(),
                header_size,
                PROT_READ | PROT_WRITE,
                MAP_SHARED,
                fd,
                0,
            )
        };
        if header_map == MAP_FAILED {
            let err = io::Error::last_os_error();
            unsafe {
                close(fd);
            }
            return Err(err);
        }

        let header_ref = unsafe { &*(header_map as *const ShmHeader) };
        let item_size = header_ref.item_size;
        let slot_size = header_ref.slot_size;
        let slots_offset = header_ref.slots_offset;
        let capacity = header_ref.capacity;
        let mask = header_ref.mask;

        if item_size != mem::size_of::<T>() {
            unsafe {
                munmap(header_map, header_size);
                close(fd);
            }
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "type size mismatch for shared memory queue",
            ));
        }

        if slot_size != mem::size_of::<Slot<T>>() {
            unsafe {
                munmap(header_map, header_size);
                close(fd);
            }
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "slot size mismatch for shared memory queue",
            ));
        }

        let total_size = slots_offset
            .checked_add(capacity.checked_mul(slot_size).ok_or_else(|| {
                io::Error::new(io::ErrorKind::InvalidData, "capacity too large in header")
            })?)
            .ok_or_else(|| {
                io::Error::new(io::ErrorKind::InvalidData, "layout overflow in header")
            })?;

        if total_size < header_size {
            unsafe {
                munmap(header_map, header_size);
                close(fd);
            }
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "shared memory backing size is too small",
            ));
        }

        unsafe {
            munmap(header_map, header_size);
        }

        let ptr = unsafe {
            mmap(
                ptr::null_mut(),
                total_size,
                PROT_READ | PROT_WRITE,
                MAP_SHARED,
                fd,
                0,
            )
        };
        if ptr == MAP_FAILED {
            let err = io::Error::last_os_error();
            unsafe {
                close(fd);
            }
            return Err(err);
        }

        let header_ptr = ptr as *mut ShmHeader;
        let slots_ptr = unsafe { (ptr as *mut u8).add(slots_offset) as *mut Slot<T> };

        Ok(Self {
            header: header_ptr,
            slots: slots_ptr,
            fd,
            layout_size: total_size,
            capacity,
            mask,
            _marker: PhantomData,
        })
    }

    /// Attempts to push a value into the buffer. Returns [`TryPushError::Full`] if the queue is full.
    pub fn try_push(&self, value: T) -> Result<(), TryPushError> {
        let enqueue = unsafe { &(*self.header).enqueue_pos };
        loop {
            let pos = enqueue.load(Ordering::Relaxed);
            let slot = unsafe { &*self.slots.add(pos & self.mask) };
            let seq = slot.sequence.load(Ordering::Acquire);
            let dif = seq as isize - pos as isize;

            if dif == 0 {
                if enqueue
                    .compare_exchange_weak(
                        pos,
                        pos.wrapping_add(1),
                        Ordering::Acquire,
                        Ordering::Relaxed,
                    )
                    .is_ok()
                {
                    unsafe {
                        (*slot.value.get()).write(value);
                    }
                    slot.sequence.store(pos.wrapping_add(1), Ordering::Release);
                    return Ok(());
                }
            } else if dif < 0 {
                return Err(TryPushError::Full);
            } else {
                thread::yield_now();
            }
        }
    }

    /// Attempts to pop a value from the buffer. Returns [`TryPopError::Empty`] if no data is available.
    pub fn try_pop(&self) -> Result<T, TryPopError> {
        let dequeue = unsafe { &(*self.header).dequeue_pos };
        loop {
            let pos = dequeue.load(Ordering::Relaxed);
            let slot = unsafe { &*self.slots.add(pos & self.mask) };
            let seq = slot.sequence.load(Ordering::Acquire);
            let dif = seq as isize - (pos.wrapping_add(1) as isize);

            if dif == 0 {
                if dequeue
                    .compare_exchange_weak(
                        pos,
                        pos.wrapping_add(1),
                        Ordering::Acquire,
                        Ordering::Relaxed,
                    )
                    .is_ok()
                {
                    let value = unsafe { (*slot.value.get()).assume_init_read() };
                    slot.sequence
                        .store(pos.wrapping_add(self.mask + 1), Ordering::Release);
                    return Ok(value);
                }
            } else if dif < 0 {
                return Err(TryPopError::Empty);
            } else {
                thread::yield_now();
            }
        }
    }

    /// Returns the configured capacity of the buffer.
    pub fn capacity(&self) -> usize {
        self.capacity
    }

    /// Unlinks a shared memory queue with the provided name.
    pub fn unlink(name: &str) -> io::Result<()> {
        let c_name = to_cstring(name)?;
        let res = unsafe { shm_unlink(c_name.as_ptr()) };
        if res != 0 {
            Err(io::Error::last_os_error())
        } else {
            Ok(())
        }
    }
}

impl<T> Drop for ShmRingBuffer<T> {
    fn drop(&mut self) {
        unsafe {
            if !self.header.is_null() && self.layout_size > 0 {
                munmap(self.header as *mut c_void, self.layout_size);
            }
            if self.fd >= 0 {
                close(self.fd);
            }
        }
    }
}

fn align_up(value: usize, align: usize) -> usize {
    if align == 0 {
        value
    } else {
        (value + align - 1) & !(align - 1)
    }
}

fn to_cstring(name: &str) -> io::Result<CString> {
    if name.is_empty() {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "shared memory name must not be empty",
        ));
    }

    let mut actual = String::new();
    if !name.starts_with('/') {
        actual.push('/');
    }
    actual.push_str(name);

    CString::new(actual).map_err(|_| {
        io::Error::new(
            io::ErrorKind::InvalidInput,
            "shared memory name contains interior null bytes",
        )
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
    use std::sync::{Arc, Mutex};
    use std::time::Duration;

    fn unique_name() -> String {
        static NEXT_ID: AtomicUsize = AtomicUsize::new(0);
        let id = NEXT_ID.fetch_add(1, Ordering::Relaxed);
        format!("demo_shm_queue_{}_{}", std::process::id(), id)
    }

    #[test]
    fn single_thread_roundtrip() {
        let name = unique_name();
        let queue = ShmRingBuffer::<u32>::create(&name, 8).unwrap();
        assert_eq!(queue.capacity(), 8);

        queue.try_push(42).unwrap();
        assert_eq!(queue.try_pop().unwrap(), 42);

        ShmRingBuffer::<u32>::unlink(&name).unwrap();
    }

    #[test]
    fn reopen_existing_queue() {
        let name = unique_name();
        {
            let queue = ShmRingBuffer::<u64>::create(&name, 16).unwrap();
            queue.try_push(5).unwrap();
            assert_eq!(queue.try_pop().unwrap(), 5);
        }

        let reopened = ShmRingBuffer::<u64>::open(&name).unwrap();
        reopened.try_push(11).unwrap();
        assert_eq!(reopened.try_pop().unwrap(), 11);
        ShmRingBuffer::<u64>::unlink(&name).unwrap();
    }

    #[test]
    fn mpmc_stress() {
        let name = unique_name();
        let queue = Arc::new(ShmRingBuffer::<u32>::create(&name, 64).unwrap());
        let producers: usize = 4;
        let consumers: usize = 4;
        let items_per_producer: usize = 512;
        let total_items = producers * items_per_producer;

        let results = Arc::new(Mutex::new(Vec::with_capacity(total_items)));
        let done = Arc::new(AtomicBool::new(false));
        let produced = Arc::new(AtomicUsize::new(0));
        let consumed = Arc::new(AtomicUsize::new(0));

        let mut producer_handles = Vec::new();
        for p in 0..producers {
            let queue = Arc::clone(&queue);
            let produced = Arc::clone(&produced);
            producer_handles.push(thread::spawn(move || {
                let start = p * items_per_producer as usize;
                let end = start + items_per_producer as usize;
                for value in start..end {
                    loop {
                        if queue.try_push(value as u32).is_ok() {
                            produced.fetch_add(1, Ordering::AcqRel);
                            break;
                        } else {
                            thread::yield_now();
                        }
                    }
                }
            }));
        }

        let mut consumer_handles = Vec::new();
        for _ in 0..consumers {
            let queue = Arc::clone(&queue);
            let results = Arc::clone(&results);
            let done = Arc::clone(&done);
            let produced = Arc::clone(&produced);
            let consumed = Arc::clone(&consumed);
            consumer_handles.push(thread::spawn(move || {
                loop {
                    match queue.try_pop() {
                        Ok(v) => {
                            {
                                let mut guard = results.lock().unwrap();
                                guard.push(v);
                            }
                            if consumed.fetch_add(1, Ordering::AcqRel) + 1 == total_items {
                                break;
                            }
                        }
                        Err(TryPopError::Empty) => {
                            if done.load(Ordering::Acquire)
                                && produced.load(Ordering::Acquire) == total_items
                                && consumed.load(Ordering::Acquire) >= total_items
                            {
                                break;
                            }
                            thread::sleep(Duration::from_micros(50));
                        }
                    }
                }
            }));
        }

        for handle in producer_handles {
            handle.join().unwrap();
        }
        done.store(true, Ordering::Release);

        for handle in consumer_handles {
            handle.join().unwrap();
        }

        let mut guard = results.lock().unwrap();
        guard.sort_unstable();
        for (idx, value) in guard.iter().copied().enumerate() {
            assert_eq!(value as usize, idx);
        }
        assert_eq!(guard.len(), total_items);

        drop(guard);
        ShmRingBuffer::<u32>::unlink(&name).unwrap();
    }
}
