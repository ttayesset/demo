use std::alloc::Layout;
use std::cell::UnsafeCell;
use std::error::Error;
use std::fmt;
use std::marker::PhantomData;
use std::mem::MaybeUninit;
use std::ptr;
use std::sync::atomic::{AtomicUsize, Ordering};

use shared_memory::{Shmem, ShmemConf, ShmemError};

/// Errors that can occur when creating or opening a shared queue.
#[derive(Debug)]
pub enum QueueError {
    /// The provided capacity was zero.
    CapacityZero,
    /// The provided capacity was not a power of two.
    CapacityNotPowerOfTwo,
    /// The queue was too large to fit into addressable memory.
    LayoutTooLarge,
    /// The underlying shared memory region could not be created or opened.
    Shmem(ShmemError),
    /// The shared memory region did not contain a valid queue header.
    CorruptedSharedMemory,
}

impl From<ShmemError> for QueueError {
    fn from(value: ShmemError) -> Self {
        QueueError::Shmem(value)
    }
}

impl fmt::Display for QueueError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            QueueError::CapacityZero => write!(f, "queue capacity must be greater than zero"),
            QueueError::CapacityNotPowerOfTwo => {
                write!(f, "queue capacity must be a power of two")
            }
            QueueError::LayoutTooLarge => write!(f, "queue layout is too large to allocate"),
            QueueError::Shmem(err) => write!(f, "shared memory error: {err}"),
            QueueError::CorruptedSharedMemory => {
                write!(f, "the shared memory region does not contain a valid queue")
            }
        }
    }
}

impl Error for QueueError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            QueueError::Shmem(err) => Some(err),
            _ => None,
        }
    }
}

/// Error returned when pushing into a full queue.
#[derive(Debug)]
pub enum PushError<T> {
    /// The queue was full and the value could not be inserted.
    Full(T),
}

impl<T: fmt::Debug> fmt::Display for PushError<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            PushError::Full(value) => write!(f, "queue is full; failed to push {value:?}"),
        }
    }
}

impl<T: fmt::Debug> Error for PushError<T> {}

/// Error returned when popping from an empty queue.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PopError {
    /// The queue had no available items.
    Empty,
}

impl fmt::Display for PopError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            PopError::Empty => write!(f, "queue is empty"),
        }
    }
}

impl Error for PopError {}

#[repr(C)]
struct SharedHeader {
    capacity: usize,
    mask: usize,
    enqueue_pos: AtomicUsize,
    dequeue_pos: AtomicUsize,
}

impl SharedHeader {
    fn new(capacity: usize) -> Self {
        SharedHeader {
            capacity,
            mask: capacity - 1,
            enqueue_pos: AtomicUsize::new(0),
            dequeue_pos: AtomicUsize::new(0),
        }
    }
}

#[repr(C)]
struct Slot<T: Copy> {
    sequence: AtomicUsize,
    value: UnsafeCell<MaybeUninit<T>>,
}

impl<T: Copy> Slot<T> {
    fn new(index: usize) -> Self {
        Slot {
            sequence: AtomicUsize::new(index),
            value: UnsafeCell::new(MaybeUninit::uninit()),
        }
    }

    unsafe fn write(&self, value: T) {
        (*self.value.get()).write(value);
    }

    unsafe fn read(&self) -> T {
        (*self.value.get()).assume_init_read()
    }
}

unsafe impl<T: Copy> Send for Slot<T> {}
unsafe impl<T: Copy> Sync for Slot<T> {}

/// A lock-free multi-producer multi-consumer queue backed by shared memory.
pub struct SharedMpmcQueue<T: Copy> {
    shmem: Shmem,
    header: *mut SharedHeader,
    slots: *mut Slot<T>,
    capacity: usize,
    mask: usize,
    _marker: PhantomData<T>,
}

unsafe impl<T: Copy> Send for SharedMpmcQueue<T> {}
unsafe impl<T: Copy> Sync for SharedMpmcQueue<T> {}

impl<T: Copy> SharedMpmcQueue<T> {
    /// Create a new queue with the provided name and capacity.
    ///
    /// The capacity must be a power of two.
    pub fn create(name: &str, capacity: usize) -> Result<Self, QueueError> {
        if capacity == 0 {
            return Err(QueueError::CapacityZero);
        }
        if !capacity.is_power_of_two() {
            return Err(QueueError::CapacityNotPowerOfTwo);
        }

        let (layout, slots_offset) = queue_layout::<T>(capacity)?;
        let shmem = ShmemConf::new()
            .flink(name)
            .size(layout.size())
            .create()
            .map_err(QueueError::from)?;

        unsafe {
            initialize_region::<T>(&shmem, capacity, slots_offset);
        }

        Self::from_shmem(shmem)
    }

    /// Open an existing queue.
    pub fn open(name: &str) -> Result<Self, QueueError> {
        let shmem = ShmemConf::new()
            .flink(name)
            .open()
            .map_err(QueueError::from)?;
        Self::from_shmem(shmem)
    }

    /// Remove the shared memory object backing the queue.
    pub fn unlink(name: &str) -> Result<(), QueueError> {
        ShmemConf::new()
            .flink(name)
            .unlink()
            .map_err(QueueError::from)
    }

    fn from_shmem(shmem: Shmem) -> Result<Self, QueueError> {
        let base = shmem.as_ptr();
        if base.is_null() {
            return Err(QueueError::CorruptedSharedMemory);
        }
        let header_ptr = base as *mut SharedHeader;
        let header = unsafe { &*header_ptr };
        let capacity = header.capacity;
        if capacity == 0 || !capacity.is_power_of_two() || header.mask != capacity - 1 {
            return Err(QueueError::CorruptedSharedMemory);
        }

        let (layout, slots_offset) = queue_layout::<T>(capacity)?;
        if shmem.len() < layout.size() {
            return Err(QueueError::CorruptedSharedMemory);
        }

        let slots_ptr = unsafe { base.add(slots_offset) as *mut Slot<T> };

        Ok(SharedMpmcQueue {
            shmem,
            header: header_ptr,
            slots: slots_ptr,
            capacity,
            mask: capacity - 1,
            _marker: PhantomData,
        })
    }

    #[inline]
    fn header(&self) -> &SharedHeader {
        unsafe { &*self.header }
    }

    #[inline]
    fn slot(&self, index: usize) -> &Slot<T> {
        unsafe { &*self.slots.add(index & self.mask) }
    }

    /// Returns the capacity of the queue.
    pub fn capacity(&self) -> usize {
        self.capacity
    }

    /// Returns an approximate length of the queue.
    pub fn len(&self) -> usize {
        let head = self.header().dequeue_pos.load(Ordering::Acquire);
        let tail = self.header().enqueue_pos.load(Ordering::Acquire);
        tail.saturating_sub(head)
    }

    /// Returns `true` if the queue is likely to be empty.
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Attempt to push a value into the queue without blocking.
    pub fn try_push(&self, value: T) -> Result<(), PushError<T>> {
        loop {
            let pos = self.header().enqueue_pos.load(Ordering::Relaxed);
            let slot = self.slot(pos);
            let seq = slot.sequence.load(Ordering::Acquire);
            let diff = seq as isize - pos as isize;

            if diff == 0 {
                if self
                    .header()
                    .enqueue_pos
                    .compare_exchange_weak(pos, pos + 1, Ordering::Relaxed, Ordering::Relaxed)
                    .is_ok()
                {
                    unsafe {
                        slot.write(value);
                    }
                    slot.sequence.store(pos + 1, Ordering::Release);
                    return Ok(());
                }
            } else if diff < 0 {
                return Err(PushError::Full(value));
            } else {
                std::hint::spin_loop();
            }
        }
    }

    /// Attempt to pop a value from the queue without blocking.
    pub fn try_pop(&self) -> Result<T, PopError> {
        loop {
            let pos = self.header().dequeue_pos.load(Ordering::Relaxed);
            let slot = self.slot(pos);
            let seq = slot.sequence.load(Ordering::Acquire);
            let diff = seq as isize - (pos + 1) as isize;

            if diff == 0 {
                if self
                    .header()
                    .dequeue_pos
                    .compare_exchange_weak(pos, pos + 1, Ordering::Relaxed, Ordering::Relaxed)
                    .is_ok()
                {
                    let value = unsafe { slot.read() };
                    slot.sequence.store(pos + self.capacity, Ordering::Release);
                    return Ok(value);
                }
            } else if diff < 0 {
                return Err(PopError::Empty);
            } else {
                std::hint::spin_loop();
            }
        }
    }
}

fn queue_layout<T: Copy>(capacity: usize) -> Result<(Layout, usize), QueueError> {
    let header_layout = Layout::new::<SharedHeader>();
    let slots_layout =
        Layout::array::<Slot<T>>(capacity).map_err(|_| QueueError::LayoutTooLarge)?;
    let (layout, slots_offset) = header_layout
        .extend(slots_layout)
        .map_err(|_| QueueError::LayoutTooLarge)?;
    Ok((layout.pad_to_align(), slots_offset))
}

unsafe fn initialize_region<T: Copy>(shmem: &Shmem, capacity: usize, slots_offset: usize) {
    let base = shmem.as_ptr();
    let header_ptr = base as *mut SharedHeader;
    ptr::write(header_ptr, SharedHeader::new(capacity));

    let slots_ptr = base.add(slots_offset) as *mut Slot<T>;
    for index in 0..capacity {
        ptr::write(slots_ptr.add(index), Slot::new(index));
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::{Arc, Barrier};
    use std::thread;
    use std::time::{SystemTime, UNIX_EPOCH};

    fn unique_name(suffix: &str) -> String {
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        format!("/shared-mpmc-{suffix}-{nanos}")
    }

    #[test]
    fn basic_roundtrip() {
        let name = unique_name("basic");
        let _ = SharedMpmcQueue::<u32>::unlink(&name);
        let writer = SharedMpmcQueue::create(&name, 8).unwrap();
        let reader = SharedMpmcQueue::<u32>::open(&name).unwrap();

        writer.try_push(42).unwrap();
        assert_eq!(reader.try_pop().unwrap(), 42);
        assert!(reader.try_pop().is_err());

        SharedMpmcQueue::<u32>::unlink(&name).unwrap();
    }

    #[test]
    fn multi_threaded_stress() {
        const PRODUCERS: usize = 2;
        const CONSUMERS: usize = 2;
        const TOTAL_MESSAGES: usize = 1_000;
        const CAPACITY: usize = 64;

        let name = unique_name("stress");
        let _ = SharedMpmcQueue::<u64>::unlink(&name);
        let queue = SharedMpmcQueue::create(&name, CAPACITY).unwrap();
        let barrier = Arc::new(Barrier::new(PRODUCERS + CONSUMERS));
        let produced = Arc::new(AtomicUsize::new(0));
        let consumed = Arc::new(AtomicUsize::new(0));

        let mut handles = Vec::new();

        for _ in 0..PRODUCERS {
            let queue = SharedMpmcQueue::open(&name).unwrap();
            let barrier = barrier.clone();
            let produced = produced.clone();
            handles.push(thread::spawn(move || {
                barrier.wait();
                for _ in 0..(TOTAL_MESSAGES / PRODUCERS) {
                    let message = produced.fetch_add(1, Ordering::Relaxed) as u64;
                    loop {
                        if queue.try_push(message).is_ok() {
                            break;
                        }
                        thread::yield_now();
                    }
                }
            }));
        }

        for _ in 0..CONSUMERS {
            let queue = SharedMpmcQueue::open(&name).unwrap();
            let barrier = barrier.clone();
            let consumed = consumed.clone();
            handles.push(thread::spawn(move || {
                barrier.wait();
                loop {
                    let current = consumed.load(Ordering::Relaxed);
                    if current >= TOTAL_MESSAGES {
                        break;
                    }
                    match queue.try_pop() {
                        Ok(_) => {
                            consumed.fetch_add(1, Ordering::Relaxed);
                        }
                        Err(PopError::Empty) => thread::yield_now(),
                    }
                }
            }));
        }

        for handle in handles {
            handle.join().unwrap();
        }

        assert_eq!(consumed.load(Ordering::Relaxed), TOTAL_MESSAGES);
        drop(queue);
        SharedMpmcQueue::<u64>::unlink(&name).unwrap();
    }
}
