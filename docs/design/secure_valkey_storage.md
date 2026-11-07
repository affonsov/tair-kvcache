# Secure Valkey block storage

The Valkey KV block data path supports a TLS endpoint with named ACL authentication:

```json
{
  "type": "valkey",
  "global_unique_name": "valkey-primary",
  "storage_spec": {
    "endpoint": "valkeys://cache.internal:16379/?db=11",
    "security_dir": "/run/kvcm/valkey-primary"
  }
}
```

`security_dir` must be an absolute path mounted into both KVCache Manager and every worker that uses `TransferClient`. The client opens the directory with `O_DIRECTORY | O_CLOEXEC | O_NOFOLLOW`; the final path component must therefore be a real directory rather than a symlink. The directory must be owned by the process effective UID or by root and must not be group- or world-writable. Modes such as `0700`, `0750`, and `0755` satisfy this policy.

For a non-root process using a projected secret volume whose entries are symlinks, use a trusted init step to copy the three fixed files into a private real directory on an empty volume. The init step should address only the fixed names, set the destination directory and files to ownership trusted by the main process, and then set `ca.pem` and `username` to `0444` and `password` to `0400` or `0600`. The password file must be readable by and normally owned by the runtime UID; a root-owned `0400` password file is not readable by a non-root process. The main process should use that real destination directory as `security_dir`; it should not use the projected symlink directory directly.

The client opens these fixed names relative to the validated directory descriptor with `openat` and `O_NOFOLLOW | O_NONBLOCK | O_CLOEXEC`:

- `ca.pem`: a nonempty regular CA chain of at most 1 MiB.
- `username`: a nonempty regular named ACL username of at most 256 bytes. One trailing newline is accepted.
- `password`: a nonempty regular ACL password of at most 4096 bytes. One trailing newline is accepted.

Each file must be owned by the process effective UID or by root and must have at least one read bit. Operating-system access checks still apply: for a non-root process, the password file must be readable by and normally owned by the runtime UID. Root-owned `0400` works only for a root process. Group and world write or execute bits are rejected for every file. `ca.pem` and `username` may be group- or world-readable, so read-only secret-volume modes such as `0444` are accepted. `password` must have no group or world permission bits; use `0400` or `0600`. Symlinks, directories, devices, FIFOs, sockets, empty files, oversized files, and unsafe modes are rejected.

The directory descriptor and validated CA descriptor remain open while the TLS context is created. Hiredis receives an OS file-descriptor path for that already-open CA file, rather than the configured pathname, and the client compares the directory and CA identities after context creation. The trusted directory mode also prevents an untrusted group or other user from renaming its fixed entries. The client fails closed if the platform cannot provide a stable file-descriptor path. The effective UID and root remain trusted administrators and can intentionally replace mounted material.

The client verifies the CA chain and matches the certificate DNS name or IP SAN against the endpoint host. DNS endpoints also use that host for SNI. The current endpoint host token grammar is one or more letters, digits, dots (`.`), hyphens (`-`), or underscores (`_`), followed by `:<port>`; parsing does not claim that every accepted token is a valid DNS name. IPv4 literals fit this grammar and are accepted. Bracketed IPv6 literals are not supported. Verification cannot be disabled, and a failed TLS connection never falls back to plaintext. The username and password are read again for each new connection or reconnect. Only the endpoint and `security_dir` reference are serialized. Credential values are never placed in source, a URI, JSON, protobuf, a data-location URI, logs, errors, environment variables, or process arguments. Log-oriented string rendering redacts both the endpoint and the directory path.

The ACL user should allow only `PING`, `SELECT`, `GET`, `SET`, `DEL`, `EXISTS`, `GETRANGE`, and `EVAL_RO`, deny commands such as `DBSIZE`, and restrict keys to the storage pattern. `EVAL_RO` runs the client's fixed read-only bounded-read script, which calls only `EXISTS` and `GETRANGE` on the declared key; Valkey checks those inner calls against the same ACL user:

```text
kvcm:valkey:<storage-name-byte-count>:<storage-name>:*
```

Valkey ACL key patterns span all logical databases. Granting `+select` permits selection of any numeric logical database supported by the instance; ACL rules cannot restrict `SELECT` to the database number in the endpoint. The `db` query parameter is a compatibility and data-partitioning setting, not a security boundary. Strict tenant isolation requires separate Valkey instances and credentials, plus network policy that permits each tenant to reach only its intended instance.

Keep Valkey on a private network. Local tests bind it to `127.0.0.1` without publishing a container port. TLS and ACLs do not make internet exposure acceptable.

Plain `valkey://` and compatibility `redis://` endpoints keep their existing behavior and must not set `security_dir`. Secure `valkeys://` is only a connection endpoint. Generated block locations remain credential-free `valkey://<storage-name>/<opaque-id>?size=<bytes>` URIs.

This data-path support does not add TLS or ACL handling to registry, metadata, async metadata, or coordination Redis clients. It also excludes mTLS and client certificates, immediate pool-wide credential rotation, secret-provider integration, and control-plane role changes.
