# ezf-plugin-worker (Phase 8 Linux sandbox)

`ezf-plugin-worker` is the process-isolated runtime for one plugin shared library. It uses ABI V2
and `PluginExecutionCore`, speaks the bounded `PluginIpc` protocol, and forwards database/logging
capabilities to the host broker. The worker does not host the backend HTTP API or own the plugin
registry.

## Development-only mode

Local integration tests may explicitly use:

```bash
ezf-plugin-worker --ipc-socket /absolute/path/to/private.sock --allow-unconfined
```

The host must separately opt in with `EZF_PLUGIN_ALLOW_UNCONFINED=1`. This mode reports
`sandbox_enforced = false`; it is not a production security boundary.

## Enforced Linux profiles

Enforced profiles are supported on Linux x86_64 and aarch64. The worker installs the sandbox
before loading plugin code, reports the active/missing controls over `SandboxReady`, and exits on
setup failure rather than silently falling back to unconfined execution.

- `DomainPlugin`: Landlock read-only filesystem allowlist, private writable plugin temp directory,
  seccomp restrictions on direct networking, process creation, privilege/capability changes and
  cross-process signals, `no_new_privs`, capability-set verification, inherited-FD reduction and
  `RLIMIT` limits. Database access must use the host capability broker.
- `DataSourcePlugin`: the same filesystem/process restrictions, plus Landlock TCP-connect port
  allowlisting. Its IP-level egress allowlist and any non-TCP egress restriction **must be enforced
  by the deployment firewall/container/network namespace**. The worker only checks that the
  operator acknowledged this requirement; it cannot verify the firewall rules.
- Non-Linux enforced mode is rejected. A Linux kernel without the required Landlock ABI or seccomp
  support is rejected. DataSource requires Landlock ABI 4 or later for TCP port rules.

The worker must run as a non-root user with all Linux capability sets empty. Configure it under a
dedicated worker identity/user namespace where feasible. The worker also reports deployment-managed
requirements for cgroup CPU/memory limits and a total quota for the temp filesystem. `RLIMIT_FSIZE`
limits each individual file, not total temp storage. The status is therefore not a claim that these
external controls are active.

## Host configuration

Process mode remains explicitly selected. Use a per-plugin profile map when both Domain and
DataSource plugins are loaded; map keys must match the canonical absolute shared-library path:

```bash
export EZF_PLUGIN_EXECUTOR=process
export EZF_PLUGIN_WORKER_PATH=/absolute/path/to/ezf-plugin-worker
export EZF_PLUGIN_SANDBOX_PROFILES='{"/opt/ezf/plugins/VariantFilter/libvariant_filter.so":"domain_plugin","/opt/ezf/plugins/MolgenDB/libmolgendb.so":"data_source_plugin"}'
```

For a Domain-only development/staging installation, `EZF_PLUGIN_SANDBOX_PROFILE=domain_plugin`
can be used instead of a map. Avoid `EZF_PLUGIN_ALLOW_UNCONFINED=1` except in local development.

Optional worker file/library access is explicit:

- `EZF_PLUGIN_SANDBOX_READ_PATHS`: OS path-list separated list of specific config files or narrowly
  scoped directories. Files are read-only and do not automatically receive library execute rights.
  `/`, `/proc`, `/sys`, `/dev`, `/run`, and broad `/etc`, `/home`, `/root`, `/var`, `/tmp` roots
  are rejected.
- `EZF_PLUGIN_SANDBOX_LIBRARY_PATHS`: OS path-list separated list of trusted, non-group/world-
  writable library directories. These are the only paths used to synthesize the worker's
  `LD_LIBRARY_PATH`; inherited loader overrides are not copied.
- `EZF_PLUGIN_WORKER_ENV_ALLOWLIST`: comma-separated names of environment variables to pass to
  enforced workers. Loader overrides, `EZF_PLUGIN_*`, `PATH`, `HOME`, `TMPDIR`, `TMP`, `TEMP` and `RUSTFLAGS`
  are prohibited. Secret/database-like environment names are also rejected for Domain plugins.
  The host supplies `HOME` and `TMPDIR` as the private temp directory. DataSource secret variables
  should be allowed only for that profile and only when operationally required.

For DataSource:

```bash
export EZF_PLUGIN_DATASOURCE_EGRESS_POLICY=externally-enforced
export EZF_PLUGIN_DATASOURCE_ALLOWED_TCP_PORTS=1433
```

The TCP port list is enforced by Landlock (ABI 4+), but is not an IP allowlist. Configure firewall
or container policy to restrict **all** permitted network traffic to the database destination and
necessary DNS resolver. The host logs this as deployment-managed; if such an egress policy cannot
be guaranteed, use a separate DataSource container/service or DB broker instead.

## Resource and isolation limits

The worker applies `RLIMIT_CORE=0`, `RLIMIT_NOFILE=128` and `RLIMIT_FSIZE=64 MiB` (only lowering any
stricter inherited limit), sets `PR_SET_DUMPABLE=0` and `PR_SET_NO_NEW_PRIVS=1`, and closes inherited
file descriptors except standard descriptors and the worker IPC channels. It does not set cgroups,
create a user namespace, change UID, mount a private filesystem, or impose a total temp-storage
quota. Those controls must be configured by deployment and remain in `SandboxReady.missing_features`.

Landlock and seccomp are defense-in-depth, not a complete virtual machine. In particular, the host
capability broker's generic SQL policy is a separate authorization boundary: process isolation does
not make arbitrary SQL safe. Do not treat `sandbox_enforced = true` alone as proof that deployment
firewall, cgroup, UID/user-namespace and temp-quota controls are active.

## Integration tests

From the backend workspace root:

```bash
cargo test -p PluginWorker --test process_integration -- --test-threads=1
cargo test -p PluginHost --test dynamic_library_loader -- --test-threads=1
```

The Linux integration test either verifies denied network/file/process/signal/capability operations
and allowed private-temp writes, or verifies that the worker fails closed when a required kernel
primitive is unavailable. Run the tests on every supported Linux architecture and in the target
container image before enabling enforced mode for a plugin.
