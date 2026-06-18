# Testing this extension

The `sql` directory holds [SQLLogicTests](https://duckdb.org/dev/sqllogictest/intro.html);
`cpp` holds Catch2 unit tests; `mock` holds an LD_PRELOAD shim used to mock the kernel.

## Plain tests (`make test`)

```bash
make release     # build first (the mock suite needs the static lib)
make test        # SQLLogicTests, then the mock suite (test_release + test_mock)
```

`make test` runs the SQLLogicTests under `test/sql/` and then the mock-backed C++ suite
(see below). The SQLLogicTests, without a CWIQ FS mount, can only assert load-time
behaviour, so `cwiqduck.test` just checks the `cwiqduck` log type is registered. To run
only the SQLLogicTests, use `make test_release` (or `make test_debug`).

## Mock-backed tests (`make test_mock`)

```bash
make release        # build first
make test_mock      # runs test/run_with_mock.sh
```

This exercises the actual redirect/read/write behaviour via C++ unit tests, with **both**
external seams mocked, so no CWIQ FS mount or real S3 object is needed:

- **`getxattr`** is intercepted by an `LD_PRELOAD` shim (`test/mock/cwiqmock.c`). For
  paths listed in a config file it returns a fake `system.cwiqfs.s3_url` value (or
  injects a specific `errno`); every other call falls through to the real `getxattr`.
  This is required because the `system.*` xattr namespace cannot be set by an
  unprivileged user on a normal filesystem.
- **The "remote" S3 object** is a real local file addressed with a `file://` URL, so the
  redirect target is served by `LocalFileSystem` with no network.

`test/run_with_mock.sh` builds the shim, generates fixtures (a garbled local "mount" file
plus an intact `file://` target, and error-injection paths), writes the shim config, then
builds and runs the **C++ unit tests** (`test/cpp/test_cwiqduck.cpp`): `ConvertLocalPathToS3`
happy path and error messages (`ENODATA`/`ENOENT`/`EACCES`/`ENOTSUP`/empty), `CanHandleFile`
gating, read redirection bytes (positional reads exercise the borrow pool), write fallback
to a local handle, handle `Write`/`Truncate` throwing, and directory-op delegation.

The mount file's bytes are deliberately garbled (same length, different content) so a
correct read can only have come from the redirect target, not the on-disk file.
