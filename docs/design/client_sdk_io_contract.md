# Client SDK I/O Contract (Call Context / Hard Deadline / Buffer Lifetime)

## 1. 核心契约（一句话）

**`SdkWrapper` creates one root `SdkCallContext` at Get/Put entry. Every
accepted group receives a child context. All children share the same absolute
`std::chrono::steady_clock` deadline, cancellation flag, and caller-memory
access gate, while each child owns distinct output-commit admission,
completion, and result state.**

`GetWithContext` and `PutWithContext` belong to the separate optional
`ContextualSdkInterface`. `SdkWrapper` discovers that interface with
`dynamic_cast`; the existing `SdkInterface` virtual method order is unchanged.
Legacy backends continue through the original `Get` and `Put` methods and use
the statically injected timeout configuration.

`SdkWrapper::Init` constructs its wrapper configuration, parsed storage
configuration, running wait-task pool, SDK map, and optional owned shared-memory
file descriptor as pending state. It publishes those existing members only
after every SDK is created. Get and Put infer successful initialization from
the published pool and reject calls before publication. A function-static
lifecycle read/write lock permits concurrent Get and Put calls while serializing
Init and destruction across wrapper instances; an Init after successful
initialization is rejected.

Context-aware implementations distinguish source-read access from output-commit
access. Both acquire an RAII guard before dereferencing caller memory. Only an
output-commit admission is a result linearization point, but it is not a
completed result. The guard must call `Complete(ClientErrorCode)` after all
caller-visible writes and before release. A guard released without completion
does not publish a result. Cancellation is published first and then waits for
active guards, so no guarded caller-memory access continues after the wrapper
returns. Expired or cancelled guard acquisition fails closed. Once an output
commit is admitted before its deadline, it finishes under that guard without a
later per-IOV deadline check.

## 2. 机制分层

| 层 | 职责 |
|---|---|
| SdkWrapper | Create the root absolute deadline at wrapper entry; create one child context per accepted group; submit with nonblocking `TryAsync`; preserve bounded drain and static timeout injection for legacy SDKs; publish stop and shared cancellation before draining accepted futures; synchronize guarded caller-memory access before every return |
| Context-aware storage backend | Consume the unchanged shared absolute deadline for admission and I/O; guard every caller-memory gather, scatter, and output publication; complete each output guard with the published result |
| Legacy storage backend | Use the original ABI and statically injected timeout budget; legacy calls publish no contextual output-commit result and retain their documented hard/soft buffer-lifetime behavior |

Queue time is part of the context deadline. A backend must not derive a fresh
relative timeout when its worker starts.

## 3. 后端履约矩阵

| 后端 | 有界性 | 准入检查 | Buffer 级别 | 超时行为 |
|---|---|---|---|---|
| LocalFile / NFS | 逐 block | 组级 + 逐 block | hard | abort 路径 sync GPU stream（GpuStreamDrainGuard），真取消 |
| HF3FS | hf3fs_wait_for_ios(abs_timeout) | 逐 block | hard | 数据先落自有 shm iov；超时不执行 CopyIovs；泄漏 iov/IOR 而不 free（free = UAF） |
| Mooncake | 逐 key 前置检查 | 逐 key | soft | 上游无取消语义；超时路径输出可归因日志（key/buffer/elapsed） |
| Valkey | Wrapper-entry absolute deadline for pool and socket I/O | Batch admission plus per-command PR2 checks | hard | Pool clients are prepared during SDK initialization so operation-time reconnects use cached numeric addresses. Contextual calls use `AcquireClientUntil`, `SetStringsUntil`, and bounded `EVAL_RO` `GetStringsUntil` with the unchanged deadline. Put allocates owned destination strings before source admission, copies caller IOVs in 64 KiB chunks with a live check between chunks, and completes output guards after scatter/publication. |
| TairMempool (PACE) | PACE 内部超时（10s） | PACE 自管理 | hard（staging + cancel_and_drain） | **不读取注入的预算**（自行管理内部超时）；要求内部超时 < wrapper 预算（10s < 15s） |

注：PACE 属跨仓库生产路径，本仓库不修改其实现；注入字段对它是纯增量（不读即无行为
变化），未来若希望显式对齐可读取 `timeout_config()` 校验自身配置。

## 4. Known Limitations

1. **Queue time is charged to contextual calls**: `SdkWrapper` creates the
   deadline before validation, grouping, and queueing. Valkey consumes that same
   absolute deadline. Legacy backends still use their injected relative budget,
   so their existing queue-time limitation remains unchanged.
2. **ManagerClient / RTPLLMClient 层**：走 TransferClient 的既有路径，自动获得
   wrapper 预算，无额外配置。
3. **Timeout, error, and submission draining**: `RunWithTimeoutParallel` uses
   nonblocking `TryAsync`; queue-full and stopped-pool rejections never execute a
   task inline. After any task was accepted, timeout, error, rejection, or
   submission failure publishes stop and shared cancellation before bounded
   draining. One precedence helper then scans every accepted future outcome and
   every child context's completed commit result. A reported timeout, or a result
   that remains unknown at the deadline, takes precedence over thread-pool and
   ordinary SDK errors. `ER_TASK_CANCELLED` is internal and is never returned.
   A Get result is preserved across deadline cancellation only when every
   accepted group has admitted and completed its own output commit with a known
   result. A completed child commit remains readable after caller-memory guards
   drain even if the task callable has not yet fulfilled its future. One group's
   commit never validates a legacy or incomplete peer.
4. **HF3FS 超时泄漏 iov/IOR**：当 DeadlineExpired 导致 WaitIos 超时时，不释放已
   提交的 I/O 的 iov 缓冲区和 IOR（释放会导致 UAF）。泄漏规模 = 一次超时的读写
   调用涉及的 iov 大小。线上应在 WARN 日志中观测泄漏频率，若高频则需引入 3FS
   取消机制。
5. **Mooncake 为 soft 级**：超时后在飞的 DMA 无法取消；调用方在收到超时后立即
   复用相应 buffer 是文档化的数据竞争。逐 key 准入把暴露面从整批降到 ≤1 个在飞
   key。
6. **Valkey hard-deadline and result precedence**: Expired/cancelled work is
   rejected before pool acquisition or Redis I/O. Operation-level or per-item
   `EC_TIMEOUT` returns `ER_SDK_TIMEOUT`. Any
   `ValkeyWriteCertainty::AMBIGUOUS` also returns `ER_SDK_TIMEOUT`, because write
   completion is unknown. Otherwise, malformed cardinality, operation errors,
   per-item errors, or `NOT_SENT` items return `ER_SDKWRITE_ERROR`. Put publishes
   `actual_remote_uris` only when the operation succeeds, every item is `EC_OK`
   and `CONFIRMED`, and an output commit is admitted before cancellation or the
   deadline. Get performs no scatter for an operation/per-item timeout or
   malformed operation result; for a successful operation, valid peers may still
   be scattered when other peers have only non-timeout errors. An admitted Get
   scatter or Put publication finishes under its output guard even if the clock
   crosses the deadline during that commit. `GetStringsUntil` uses bounded
   `EVAL_RO` reads capped at each expected size plus one byte.
