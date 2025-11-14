# Shared memory MPMC queue

This crate provides a lock-free multi-producer multi-consumer (MPMC) ring buffer
that lives entirely in POSIX shared memory. Processes can open the same shared
memory region by name and exchange fixed-size `Copy` values without copying data
through sockets or pipes.

## Features

- Backed by the [`shared_memory`](https://crates.io/crates/shared_memory) crate
  for portable shared memory mappings.
- Lock-free algorithm inspired by the array queue used in Crossbeam.
- `try_push`/`try_pop` APIs suitable for publishers and subscribers across
  processes.
- Optional helper to unlink the shared memory region when finished.

## Running the tests

```bash
cargo test
```

## Publisher/subscriber example

Two terminals can exchange numbers using the provided example:

```bash
# Terminal 1 (publisher)
cargo run --example pub_sub -- pub 32

# Terminal 2 (subscriber)
cargo run --example pub_sub -- sub 32
```

The publisher will create the shared queue if it does not exist and push the
requested number of messages. The subscriber opens the same queue and prints
each value as it becomes available.
