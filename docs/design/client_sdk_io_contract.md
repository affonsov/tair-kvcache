# Client SDK I/O Contract: Call Context, Hard Deadline, and Buffer Lifetime

## Core contract

`SdkWrapper` creates one root `SdkCallContext` when Get or Put enters the
wrapper. Each accepted group receives a child context. All children share the
same absolute `std::chrono::steady_clock` deadline, cancellation flag, and
caller-memory access gate. Each child owns separate output-commit admission,
completion, and result state.

`GetWithContext` and `PutWithContext` belong to the optional
`ContextualSdkInterface`. `SdkWrapper` discovers that interface with
`dynamic_cast`. The existing `SdkInterface` virtual method order does not
change. Legacy backends continue through the original Get and Put methods and
use the statically injected timeout configuration.

`SdkWrapper::Init` builds its configuration, parsed storage configuration,
running wait-task pool, SDK map, and optional owned shared-memory file
descriptor as pending state. It publishes those members only after every SDK is
created. Get and Put infer successful initialization from the published pool
and reject calls before publication. A function-static lifecycle read/write
lock permits concurrent Get and Put calls while serializing Init and
destruction across wrapper instances. Init after successful initialization is
rejected.

Context-aware implementations distinguish source-read access from
output-commit access. Both acquire an RAII guard before dereferencing caller
memory. An output-commit admission is a result linearization point, but it is
not a completed result. The guard calls `Complete(ClientErrorCode)` after all
caller-visible writes and before release. Releasing a guard without Complete
does not publish a result. Cancellation is published first and then waits for
active guards, so guarded caller-memory access cannot continue after the
wrapper returns.

## Layer responsibilities

| Layer | Responsibility |
|---|---|
| `SdkWrapper` | Create the root absolute deadline at wrapper entry; create one child context per accepted group; submit with nonblocking `TryAsync`; preserve bounded drain and static timeout injection for legacy SDKs; publish stop and shared cancellation before draining accepted futures; synchronize guarded caller-memory access before every return. |
| Context-aware storage backend | Consume the unchanged shared absolute deadline for admission and I/O; guard every caller-memory gather, scatter, and output publication; complete each output guard with the published result. |
| Legacy storage backend | Use the original ABI and statically injected timeout budget; publish no contextual output-commit result; retain the documented hard or soft buffer-lifetime behavior. |

Queue time counts against a contextual call's deadline. A backend must not
derive a new relative timeout when its worker starts.

## Backend contract matrix

| Backend | Bound | Admission | Buffer lifetime | Timeout behavior |
|---|---|---|---|---|
| LocalFile / NFS | Per block | Group and per block | Hard | The abort path synchronizes its GPU stream through `GpuStreamDrainGuard`; cancellation is real. |
| HF3FS | `hf3fs_wait_for_ios(abs_timeout)` | Per block | Hard | Data first lands in owned shared-memory IOVs. A timeout does not call `CopyIovs`; submitted IOV/IOR memory is leaked because freeing it would cause a use-after-free. |
| Mooncake | Check before each key | Per key | Soft | Upstream has no cancellation operation. Timeout logs identify the key, buffer, and elapsed time. |
| Valkey | One wrapper-entry absolute deadline for pool and socket I/O | Full-request preflight, then bounded slices | Hard | Pool clients are prepared during SDK initialization, so operation-time reconnects use cached numeric addresses. Contextual calls use `AcquireClientUntil`, `SetStringsUntil`, and bounded `EVAL_RO` `GetStringsUntil` with the original deadline. Host and device copies are synchronous and finish before return. |
| TairMempool (PACE) | PACE internal 10-second timeout | PACE-managed | Hard through staging and `cancel_and_drain` | PACE does not read the injected budget. Its internal timeout must remain below the wrapper's 15-second budget. |

PACE is a production path in another repository. This repository does not
change its implementation. The injected timeout field is additive for PACE.

## Valkey bounded-slice and publication semantics

Valkey validates the complete request once before acquiring a Redis client or
performing Redis I/O. Validation covers URI identity and size, accumulated IOV
size with overflow-safe arithmetic, ignored-I/O rules, caller addresses, and
memory-type support.

After preflight, Put and Get process contiguous whole-block slices. Each slice
contains at most 64 blocks and at most 1 MiB of expected payload. Zero-length
blocks consume a block slot. A block larger than 1 MiB runs alone. Block order
and IOV order never change.

Put allocates and gathers only the current slice into ordinary owned host
strings. Each source IOV is gathered by a stateless internal bounded-copy
helper in chunks no larger than 64 KiB, with a cancellation check between
chunks. Production supplies the synchronous copy callable directly; the
helper adds no ValkeySdk data member, virtual method, callback, or runtime
state. The slice strings are released before the next slice is gathered.
`SetStringsUntil` receives the unchanged wrapper deadline for every slice. Put stops after the first timeout, ambiguous write,
server rejection, operation or item error, cancellation, or malformed result
cardinality. `actual_remote_uris` is published only after every item in every
slice reports `EC_OK` and `ValkeyWriteCertainty::CONFIRMED`.

Get calls `GetStringsUntil` once per slice and releases that slice's returned
host strings before requesting the next slice. It preserves block order, IOV
order, declared-size checks, ignored-IOV offsets, and aggregation of
non-timeout per-item errors. A timeout or malformed operation result stops
processing immediately.

Get uses one `OUTPUT_COMMIT` guard. It admits that guard immediately before the
first actual scatter, keeps it through subsequent deadline-bounded slices, and
completes it with the final overall result. A request that scatters no bytes
does not admit an output commit. Once a slice starts scattering under an
admitted guard, that synchronous scatter finishes without a per-IOV deadline
check. Before another slice begins I/O, cancellation and the original deadline
are checked again. Cancellation can therefore wait only for the current
synchronous copy and any current PR2 deadline-bounded Redis I/O. No host or
device copy remains in flight after guard release or function return.

Bounded memory requires partial publication for Get: if an earlier successful
slice has been scattered and a later slice fails or times out, the earlier
slice remains committed in caller memory. A later slice that fails during Redis
I/O, and all slices not yet processed, remain untouched. Valid items in a slice
with non-timeout per-item errors are scattered; failed or wrong-sized items
remain untouched, and the final result is `ER_SDKREAD_ERROR`. A synchronous
device-copy failure returns `ER_CUDAMEMCPY_ERROR`; bytes copied earlier in the
same scatter remain caller-visible.

## Valkey CPU and accelerator IOVs

In CPU-only builds, every nonempty, non-ignored GPU IOV is rejected during the
full preflight with `ER_UNSUPPORTED_MEMORY_TYPE`, before Redis I/O. Zero-length
IOVs remain valid and still participate in positional ordering. A nonempty
ignored Put IOV remains invalid. An ignored Get IOV advances the source offset
without touching its address.

Under `USING_CUDA`, every nonzero GPU IOV uses
`cudaPointerGetAttributes` to discover the allocation's owning device, which
also supports interior allocation pointers. The worker's current device is
captured, changed only when it differs from the owning device, and restored
after the synchronous `cudaMemcpyDeviceToHost` or
`cudaMemcpyHostToDevice`, including copy and device-switch failure paths.
`USING_MUSA` applies the same sequence with the corresponding MUSA runtime
APIs. Pointer-attribute, current-device, device-switch, copy, and restoration
failures map to `ER_CUDAMEMCPY_ERROR`; logs contain only the operation, API,
and runtime error metadata. Defining CUDA and MUSA together is a compilation
error.

CPU and GPU IOVs can be mixed in any order, and successive IOVs may belong to
different devices. Zero-size GPU IOVs and CPU-only IOVs make no accelerator
runtime calls. Ignored Get GPU IOVs advance the source offset without querying
or touching their addresses. All device copies are synchronous, and the
worker's original current device is restored before the contextual call
returns.

The maintained vLLM, SGLang, and TensorRT-LLM connectors already submit CPU
IOVs because their connector paths stage through host buffers. Direct
`TransferClient` callers can now submit GPU IOVs and receive the synchronous
Valkey staging behavior above.

## Result mapping and known limitations

1. Expired or cancelled Valkey work is rejected before pool acquisition or new
   Redis I/O. Operation-level or per-item `EC_TIMEOUT` maps to
   `ER_SDK_TIMEOUT`.
2. `ValkeyWriteCertainty::AMBIGUOUS` maps to `ER_SDK_TIMEOUT`, because remote
   completion is unknown. `ValkeyWriteCertainty::REJECTED` maps to
   `ER_SDKWRITE_ERROR`. Malformed cardinality, non-timeout operation/item
   errors, and `NOT_SENT` items also map to `ER_SDKWRITE_ERROR`.
3. `RunWithTimeoutParallel` uses nonblocking `TryAsync`. Queue-full and stopped
   pool rejections do not execute inline. After any task is accepted, timeout,
   error, rejection, or submission failure publishes stop and shared
   cancellation before bounded draining. Timeout or an outcome still unknown
   at the deadline takes precedence over thread-pool and ordinary SDK errors.
   `ER_TASK_CANCELLED` is internal and is never returned.
4. A Get result survives wrapper deadline cancellation only when every accepted
   group has a known completed output commit. One group's commit cannot validate
   an incomplete or legacy peer.
5. HF3FS intentionally leaks submitted IOV/IOR memory on timeout because freeing
   it would cause a use-after-free. Operations should monitor warning frequency
   until 3FS gains cancellation.
6. Mooncake remains a soft-lifetime backend. Reusing a timed-out buffer while
   upstream DMA is still active is a documented data race. Per-key admission
   limits exposure to at most one in-flight key.
7. ManagerClient and RTPLLMClient calls that use TransferClient receive the
   wrapper budget without extra configuration.

TLS, ACLs, credential references, and authenticated live-CI selection remain
PR5-owned. In particular, PR3 does not duplicate or alter the live Valkey CI
tag/comment used to select authenticated coverage.
