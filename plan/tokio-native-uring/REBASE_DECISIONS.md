# Tokio base version: rebased onto `tokio-1.52.3` release tag

## Decision (2026-05-22)

The patched-tokio branches `expose-uring-ops` and `multi-ring` (on
`neondatabase/tokio`) are now based on the **`tokio-1.52.3` release tag**
rather than on a snapshot of `tokio-rs/tokio` master.

## Why

- **Stable story for the funding pitch**: "we patch `tokio-1.52.3`" is
  reproducible and easy to communicate, both internally and to anyone
  evaluating the prototype's feasibility for upstream landing.
- **Stable story for eventual upstream review**: tokio maintainers will
  also prefer a release-tag base; reduces noise from unrelated drift on
  master between when the PR is opened and when reviewers look at it.
- **Reproducibility for downstream consumers** (PageServer/Hadron): a
  fixed release tag doesn't drift; master does.

## How much actually changed

Between `tokio-1.52.3` (`d8756916`) and the prior master HEAD
(`82fe082e`, "fs: clarify `create_dir_all` succeeds if path exists"),
**only one commit** touched anything under the io_uring path tree:

```
0121120b taskdump: skip double wake on `Trace::capture`/`Trace::trace_with` (#8043)
```

…which is a taskdump fix that incidentally edits
`tokio/src/runtime/driver/op.rs`. Not API-relevant to our patch surface.

The rebase replayed our 6 patch commits onto `tokio-1.52.3` with zero
conflicts.

## Mechanics

```bash
cd ~/tokio
jj rebase -s <expose-uring-ops-base-commit> -d tokio-1.52.3
```

Used `-s` rather than `-b` so jj only rebases the commits unique to our
branches; without `-s` jj refuses because it would rewrite immutable
master history between `tokio-1.52.3` and master HEAD.

After rebase, the bookmarks moved as expected:

```
expose-uring-ops -> <new-sha> (was 97d16480, marker at b076fced)
multi-ring       -> <new-sha> (was 1a86a782, marker at 125755fc)
```

Both branches force-pushed to `neondatabase/tokio` after.

## PR description text

The tokio-native-uring PR description on
`neondatabase/tokio-epoll-uring` calls out:

> Patched tokio is based on **`tokio-1.52.3`** (release tag). The patch
> set is two stacked branches:
> - [`expose-uring-ops`](https://github.com/neondatabase/tokio/tree/expose-uring-ops):
>   public `tokio::io_uring` submission API gated on `tokio_unstable`.
> - [`multi-ring`](https://github.com/neondatabase/tokio/tree/multi-ring):
>   per-worker ring sharding driver + `Builder::io_uring_rings(n)` knob,
>   stacked on `expose-uring-ops`.
