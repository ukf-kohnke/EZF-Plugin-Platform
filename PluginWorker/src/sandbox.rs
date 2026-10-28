//! Linux-first OS sandbox for the process-isolated plugin worker.
//!
//! The enforced profiles combine Landlock filesystem/network restrictions, seccomp syscall
//! filtering, `no_new_privs`, inherited-FD reduction, and per-process rlimits. They deliberately
//! fail closed when the kernel cannot provide the requested policy. IP-level egress filtering for
//! DataSource plugins remains a deployment responsibility and must be explicitly acknowledged.

use PluginIpc::SandboxProfile;
use std::path::{ Path, PathBuf };

#[derive(Debug)]
pub(crate) struct SandboxReport {
    pub enforced: bool,
    pub active_features: Vec<String>,
    pub missing_features: Vec<String>,
}

#[cfg(target_os = "linux")]
pub(crate) fn activate(
    profile: SandboxProfile,
    plugin_path: &Path,
    plugin_directory: &Path,
    temp_directory: Option<&Path>,
    extra_read_paths: &[PathBuf],
    extra_library_paths: &[PathBuf],
    datasource_tcp_ports: &[u16],
    datasource_egress_acknowledged: bool,
    ipc_fds: &[i32]
) -> Result<SandboxReport, String> {
    use std::fs;
    use std::os::unix::fs::{ MetadataExt, PermissionsExt };

    if profile == SandboxProfile::DevelopmentUnconfined {
        return Ok(SandboxReport {
            enforced: false,
            active_features: vec!["ipc_capability_broker".to_owned(), "ipc_logging".to_owned()],
            missing_features: vec!["os_sandbox_not_implemented_for_development_profile".to_owned()],
        });
    }
    if !matches!(profile, SandboxProfile::DomainPlugin | SandboxProfile::DataSourcePlugin) {
        return Err(format!("unsupported sandbox profile: {profile:?}"));
    }

    let plugin_path = canonical_existing(plugin_path, false, "plugin library")?;
    let plugin_directory = canonical_existing(plugin_directory, true, "plugin directory")?;
    if plugin_path.parent() != Some(plugin_directory.as_path()) {
        return Err(
            "plugin library must be an immediate child of the authorized plugin directory".into()
        );
    }
    if
        ["/", "/etc", "/home", "/root", "/var", "/tmp", "/proc", "/sys", "/dev", "/run"]
            .iter()
            .any(|root| plugin_directory == Path::new(root))
    {
        return Err(
            format!(
                "plugin directory {} is too broad for an enforced sandbox",
                plugin_directory.display()
            )
        );
    }
    let plugin_dir_metadata = fs
        ::metadata(&plugin_directory)
        .map_err(|error| format!("could not inspect plugin directory: {error}"))?;
    if (plugin_dir_metadata.permissions().mode() & 0o022) != 0 {
        return Err("plugin directory must not be group- or world-writable".into());
    }
    let plugin_file_metadata = fs
        ::metadata(&plugin_path)
        .map_err(|error| format!("could not inspect plugin library: {error}"))?;
    if (plugin_file_metadata.permissions().mode() & 0o022) != 0 {
        return Err("plugin library must not be group- or world-writable".into());
    }
    let temp_directory = temp_directory.ok_or(
        "enforced sandbox requires a private plugin temp directory"
    )?;
    let temp_directory = canonical_existing(temp_directory, true, "plugin temp directory")?;
    let temp_metadata = fs
        ::metadata(&temp_directory)
        .map_err(|error| format!("could not inspect plugin temp directory: {error}"))?;
    let temp_mode = temp_metadata.permissions().mode() & 0o777;
    if (temp_mode & 0o077) != 0 || (temp_mode & 0o700) != 0o700 {
        return Err(
            "plugin temp directory must be owner-accessible (0700) and inaccessible to group/other users".into()
        );
    }
    if temp_metadata.uid() != (unsafe { libc::geteuid() }) {
        return Err("plugin temp directory must be owned by the worker effective UID".into());
    }

    if profile == SandboxProfile::DataSourcePlugin {
        if !datasource_egress_acknowledged {
            return Err(
                "DataSourcePlugin requires EZF_PLUGIN_DATASOURCE_EGRESS_POLICY=externally-enforced; IP-level egress restrictions cannot be guaranteed by seccomp alone".into()
            );
        }
        if datasource_tcp_ports.is_empty() || datasource_tcp_ports.iter().any(|port| *port == 0) {
            return Err(
                "DataSourcePlugin requires one or more explicit TCP destination ports in EZF_PLUGIN_DATASOURCE_ALLOWED_TCP_PORTS".into()
            );
        }
    }

    for path in extra_read_paths {
        validate_extra_read_path(path)?;
    }
    for path in extra_library_paths {
        validate_extra_library_path(path)?;
    }

    // Do not sandbox a privileged worker. `no_new_privs` prevents privilege gains but does not
    // revoke capabilities that were already effective at process start.
    ensure_unprivileged_identity()?;
    // The private IPC socket is already connected. Close every other inherited descriptor before
    // loading plugin code, so a plugin cannot reuse a backend-owned file/socket descriptor.
    close_inherited_file_descriptors(ipc_fds)?;
    unsafe {
        libc::umask(0o077);
    }
    apply_resource_limits()?;
    set_dumpable_disabled()?;
    set_no_new_privs()?;
    let landlock_abi = apply_landlock(
        profile,
        &plugin_path,
        &plugin_directory,
        &temp_directory,
        extra_read_paths,
        extra_library_paths,
        datasource_tcp_ports
    )?;
    apply_seccomp(profile)?;

    let mut active_features = vec![
        format!("landlock_abi_{landlock_abi}"),
        "landlock_read_only_plugin_and_runtime_paths".to_owned(),
        "landlock_private_writable_temp_directory".to_owned(),
        "seccomp_process_creation_and_privilege_syscalls_restricted".to_owned(),
        "seccomp_cross_process_signals_and_capability_mutation_denied".to_owned(),
        "prctl_no_new_privs".to_owned(),
        "dumpable_disabled".to_owned(),
        "effective_capabilities_zero".to_owned(),
        "rlimit_nofile_at_most_128".to_owned(),
        "rlimit_fsize_at_most_67108864".to_owned(),
        "rlimit_core_0".to_owned(),
        "inherited_file_descriptors_closed".to_owned()
    ];
    if landlock_abi >= 5 {
        active_features.push("landlock_device_ioctl_restricted".to_owned());
    }
    if profile == SandboxProfile::DomainPlugin {
        active_features.push("seccomp_direct_network_syscalls_denied".to_owned());
    } else {
        active_features.push("landlock_tcp_connect_port_allowlist".to_owned());
        active_features.push("seccomp_socket_domain_limited_to_inet".to_owned());
        active_features.push("external_ip_egress_policy_acknowledged_not_verified".to_owned());
    }

    let mut missing_features = vec![
        "cpu_and_memory_cgroups_must_be_configured_by_deployment".to_owned(),
        "dedicated worker UID/user namespace is deployment-managed".to_owned(),
        "plugin temp storage total quota must be enforced by deployment (RLIMIT_FSIZE limits individual files only)".to_owned()
    ];
    if profile == SandboxProfile::DataSourcePlugin {
        missing_features.push(
            "DataSource IP-level egress allowlist is deployment-managed and was not verified by the worker".to_owned()
        );
        missing_features.push(
            "DataSource non-TCP egress must be restricted by the deployment firewall".to_owned()
        );
    }
    Ok(SandboxReport { enforced: true, active_features, missing_features })
}

#[cfg(not(target_os = "linux"))]
pub(crate) fn activate(
    profile: SandboxProfile,
    _plugin_path: &Path,
    _plugin_directory: &Path,
    _temp_directory: Option<&Path>,
    _extra_read_paths: &[PathBuf],
    _extra_library_paths: &[PathBuf],
    _datasource_tcp_ports: &[u16],
    _datasource_egress_acknowledged: bool,
    _ipc_fds: &[i32]
) -> Result<SandboxReport, String> {
    if profile == SandboxProfile::DevelopmentUnconfined {
        Ok(SandboxReport {
            enforced: false,
            active_features: vec!["ipc_capability_broker".to_owned(), "ipc_logging".to_owned()],
            missing_features: vec!["os_sandbox_not_implemented_for_development_profile".to_owned()],
        })
    } else {
        Err(
            format!(
                "sandbox profile {profile:?} is not implemented on this platform; enforced startup is refused"
            )
        )
    }
}

fn _canonical_existing(
    path: &Path,
    want_directory: bool,
    description: &str
) -> Result<PathBuf, String> {
    let canonical = path
        .canonicalize()
        .map_err(|error| {
            format!("could not canonicalize {description} {}: {error}", path.display())
        })?;
    let metadata = std::fs
        ::metadata(&canonical)
        .map_err(|error| {
            format!("could not inspect {description} {}: {error}", canonical.display())
        })?;
    if want_directory && !metadata.is_dir() {
        return Err(format!("{description} must be a directory: {}", canonical.display()));
    }
    if !want_directory && !metadata.is_file() {
        return Err(format!("{description} must be a regular file: {}", canonical.display()));
    }
    Ok(canonical)
}

#[cfg(target_os = "linux")]
fn validate_extra_read_path(path: &Path) -> Result<(), String> {
    use std::os::unix::fs::PermissionsExt;
    let canonical = canonical_existing(path, path.is_dir(), "extra read-only path")?;
    validate_non_broad_extra_path(&canonical, "extra read-only")?;
    let metadata = std::fs
        ::metadata(&canonical)
        .map_err(|error|
            format!("could not inspect extra read-only path {}: {error}", canonical.display())
        )?;
    if (metadata.permissions().mode() & 0o022) != 0 {
        return Err(
            format!(
                "extra read-only path {} must not be group- or world-writable",
                canonical.display()
            )
        );
    }
    Ok(())
}

#[cfg(target_os = "linux")]
fn validate_extra_library_path(path: &Path) -> Result<(), String> {
    use std::os::unix::fs::PermissionsExt;
    let canonical = canonical_existing(path, true, "extra library path")?;
    validate_non_broad_extra_path(&canonical, "extra library")?;
    let metadata = std::fs
        ::metadata(&canonical)
        .map_err(|error|
            format!("could not inspect extra library directory {}: {error}", canonical.display())
        )?;
    if (metadata.permissions().mode() & 0o022) != 0 {
        return Err(
            format!(
                "extra library directory {} must not be group- or world-writable",
                canonical.display()
            )
        );
    }
    Ok(())
}

#[cfg(target_os = "linux")]
fn validate_non_broad_extra_path(path: &Path, label: &str) -> Result<(), String> {
    if path == Path::new("/") {
        return Err(format!("{label} paths must never grant access to the filesystem root"));
    }
    for forbidden_root in ["/proc", "/sys", "/dev", "/run"] {
        if path == Path::new(forbidden_root) || path.starts_with(forbidden_root) {
            return Err(format!("{label} path {} is not permitted", path.display()));
        }
    }
    // Avoid accidental broad grants of commonly secret-bearing or writable trees; callers may
    // grant a specific configuration file or a narrowly scoped subdirectory instead.
    for broad_root in ["/etc", "/home", "/root", "/var", "/tmp"] {
        if path == Path::new(broad_root) {
            return Err(
                format!(
                    "{label} path {broad_root} is too broad; configure a specific file or subdirectory"
                )
            );
        }
    }
    Ok(())
}

#[cfg(target_os = "linux")]
fn ensure_unprivileged_identity() -> Result<(), String> {
    use std::collections::BTreeMap;
    use std::fs;
    if (unsafe { libc::geteuid() }) == 0 {
        return Err(
            "refusing enforced plugin sandbox while worker effective UID is root; run backend/worker under an unprivileged dedicated identity".into()
        );
    }
    let status = fs
        ::read_to_string("/proc/self/status")
        .map_err(|error| {
            format!("cannot verify Linux capability sets via /proc/self/status: {error}")
        })?;
    let capability_sets: BTreeMap<&str, u64> = status
        .lines()
        .filter_map(|line| {
            let (key, value) = line.split_once(':')?;
            if !matches!(key, "CapInh" | "CapPrm" | "CapEff" | "CapBnd" | "CapAmb") {
                return None;
            }
            Some((key, u64::from_str_radix(value.trim(), 16).ok()?))
        })
        .collect();
    for key in ["CapInh", "CapPrm", "CapEff", "CapBnd", "CapAmb"] {
        let value = capability_sets
            .get(key)
            .ok_or_else(|| {
                format!("could not read {key} from /proc/self/status; refusing enforced sandbox")
            })?;
        if *value != 0 {
            return Err(
                format!(
                    "refusing enforced plugin sandbox with {key}=0x{value:x}; run worker with all Linux capability sets empty"
                )
            );
        }
    }
    Ok(())
}

#[cfg(target_os = "linux")]
fn set_dumpable_disabled() -> Result<(), String> {
    const PR_SET_DUMPABLE: libc::c_int = 4;
    if (unsafe { libc::prctl(PR_SET_DUMPABLE, 0, 0, 0, 0) }) != 0 {
        return Err(format!("PR_SET_DUMPABLE failed: {}", std::io::Error::last_os_error()));
    }
    Ok(())
}

#[cfg(target_os = "linux")]
fn apply_resource_limits() -> Result<(), String> {
    set_limit(libc::RLIMIT_CORE, 0, "RLIMIT_CORE")?;
    set_limit(libc::RLIMIT_NOFILE, 128, "RLIMIT_NOFILE")?;
    set_limit(libc::RLIMIT_FSIZE, 64 * 1024 * 1024, "RLIMIT_FSIZE")?;
    Ok(())
}

#[cfg(target_os = "linux")]
fn set_limit(resource: libc::c_int, requested: u64, name: &str) -> Result<(), String> {
    let mut current = unsafe { std::mem::zeroed::<libc::rlimit>() };
    if (unsafe { libc::getrlimit(resource, &mut current) }) != 0 {
        return Err(format!("getrlimit({name}) failed: {}", std::io::Error::last_os_error()));
    }
    let requested = requested as libc::rlim_t;
    let maximum = current.rlim_max.min(requested);
    let limit = libc::rlimit {
        rlim_cur: current.rlim_cur.min(maximum),
        rlim_max: maximum,
    };
    if (unsafe { libc::setrlimit(resource, &limit) }) != 0 {
        return Err(format!("setrlimit({name}) failed: {}", std::io::Error::last_os_error()));
    }
    Ok(())
}

#[cfg(target_os = "linux")]
fn set_no_new_privs() -> Result<(), String> {
    const PR_SET_NO_NEW_PRIVS: libc::c_int = 38;
    if (unsafe { libc::prctl(PR_SET_NO_NEW_PRIVS, 1, 0, 0, 0) }) != 0 {
        return Err(format!("PR_SET_NO_NEW_PRIVS failed: {}", std::io::Error::last_os_error()));
    }
    Ok(())
}

#[cfg(target_os = "linux")]
fn close_inherited_file_descriptors(keep: &[i32]) -> Result<(), String> {
    use std::ffi::CStr;
    let proc_fd = std::ffi::CString::new("/proc/self/fd").expect("constant has no NUL");
    let directory = unsafe { libc::opendir(proc_fd.as_ptr()) };
    if directory.is_null() {
        return Err(
            format!(
                "cannot enumerate /proc/self/fd to close inherited descriptors: {}",
                std::io::Error::last_os_error()
            )
        );
    }
    let directory_fd = unsafe { libc::dirfd(directory) };
    let mut descriptors = Vec::new();
    loop {
        let entry = unsafe { libc::readdir(directory) };
        if entry.is_null() {
            break;
        }
        let name = unsafe { CStr::from_ptr((*entry).d_name.as_ptr()) };
        if let Ok(value) = name.to_string_lossy().parse::<i32>() {
            descriptors.push(value);
        }
    }
    unsafe {
        libc::closedir(directory);
    }
    descriptors.sort_unstable();
    descriptors.dedup();
    for fd in descriptors {
        if fd > 2 && fd != directory_fd && !keep.contains(&fd) {
            // It is harmless if this is the just-closed /proc/self/fd directory descriptor.
            if (unsafe { libc::close(fd) }) != 0 {
                let error = std::io::Error::last_os_error();
                if error.raw_os_error() != Some(libc::EBADF) {
                    return Err(format!("could not close inherited descriptor {fd}: {error}"));
                }
            }
        }
    }
    Ok(())
}

#[cfg(target_os = "linux")]
fn apply_landlock(
    profile: SandboxProfile,
    plugin_path: &Path,
    plugin_directory: &Path,
    temp_directory: &Path,
    extra_read_paths: &[PathBuf],
    extra_library_paths: &[PathBuf],
    datasource_tcp_ports: &[u16]
) -> Result<i32, String> {
    const SYS_LANDLOCK_CREATE_RULESET: libc::c_long = 444;
    const SYS_LANDLOCK_ADD_RULE: libc::c_long = 445;
    const SYS_LANDLOCK_RESTRICT_SELF: libc::c_long = 446;
    const LANDLOCK_CREATE_RULESET_VERSION: libc::c_uint = 1;
    const LANDLOCK_RULE_PATH_BENEATH: libc::c_uint = 1;
    const LANDLOCK_RULE_NET_PORT: libc::c_uint = 2;

    const EXECUTE: u64 = 1 << 0;
    const WRITE_FILE: u64 = 1 << 1;
    const READ_FILE: u64 = 1 << 2;
    const READ_DIR: u64 = 1 << 3;
    const REMOVE_DIR: u64 = 1 << 4;
    const REMOVE_FILE: u64 = 1 << 5;
    const MAKE_DIR: u64 = 1 << 7;
    const MAKE_REG: u64 = 1 << 8;
    const MAKE_SOCK: u64 = 1 << 9;
    const MAKE_FIFO: u64 = 1 << 10;
    const MAKE_SYM: u64 = 1 << 12;
    const REFER: u64 = 1 << 13;
    const TRUNCATE: u64 = 1 << 14;
    const IOCTL_DEV: u64 = 1 << 15;
    const NET_BIND_TCP: u64 = 1 << 0;
    const NET_CONNECT_TCP: u64 = 1 << 1;

    #[repr(C)]
    struct RulesetAttrFs {
        handled_access_fs: u64,
    }
    #[repr(C)]
    struct RulesetAttrNet {
        handled_access_fs: u64,
        handled_access_net: u64,
    }
    #[repr(C)]
    struct NetPortAttr {
        allowed_access: u64,
        port: u64,
    }

    let abi = unsafe {
        libc::syscall(
            SYS_LANDLOCK_CREATE_RULESET,
            std::ptr::null::<libc::c_void>(),
            0usize,
            LANDLOCK_CREATE_RULESET_VERSION
        )
    };
    if abi < 1 {
        return Err(
            format!(
                "Landlock is unavailable or disabled (ABI query failed): {}",
                std::io::Error::last_os_error()
            )
        );
    }
    if profile == SandboxProfile::DataSourcePlugin && abi < 4 {
        return Err(
            format!(
                "DataSourcePlugin requires Landlock ABI >= 4 for TCP port restrictions; kernel reports ABI {abi}"
            )
        );
    }

    let mut handled_fs =
        EXECUTE |
        WRITE_FILE |
        READ_FILE |
        READ_DIR |
        REMOVE_DIR |
        REMOVE_FILE |
        (1 << 6) |
        MAKE_DIR |
        MAKE_REG |
        MAKE_SOCK |
        MAKE_FIFO |
        (1 << 11) |
        MAKE_SYM;
    if abi >= 2 {
        handled_fs |= REFER;
    }
    if abi >= 3 {
        handled_fs |= TRUNCATE;
    }
    if abi >= 5 {
        handled_fs |= IOCTL_DEV;
    }
    let handled_net = if profile == SandboxProfile::DataSourcePlugin {
        NET_BIND_TCP | NET_CONNECT_TCP
    } else {
        0
    };

    let ruleset_fd = if abi >= 4 {
        let attr = RulesetAttrNet {
            handled_access_fs: handled_fs,
            handled_access_net: handled_net,
        };
        unsafe {
            libc::syscall(
                SYS_LANDLOCK_CREATE_RULESET,
                &attr,
                std::mem::size_of::<RulesetAttrNet>(),
                0u32
            ) as i32
        }
    } else {
        let attr = RulesetAttrFs { handled_access_fs: handled_fs };
        unsafe {
            libc::syscall(
                SYS_LANDLOCK_CREATE_RULESET,
                &attr,
                std::mem::size_of::<RulesetAttrFs>(),
                0u32
            ) as i32
        }
    };
    if ruleset_fd < 0 {
        return Err(
            format!("could not create Landlock ruleset: {}", std::io::Error::last_os_error())
        );
    }

    let result = (|| {
        // The plugin bundle is readable, but only the selected entry library and explicitly
        // configured library directories receive execute rights. Arbitrary read-only config paths
        // must not automatically become executable/dlopen search roots.
        add_path_rule(ruleset_fd, plugin_path, EXECUTE | READ_FILE, LANDLOCK_RULE_PATH_BENEATH)?;
        add_path_rule(
            ruleset_fd,
            plugin_directory,
            READ_FILE | READ_DIR,
            LANDLOCK_RULE_PATH_BENEATH
        )?;

        let mut runtime_paths: Vec<PathBuf> = Vec::new();
        for path in [
            "/usr",
            "/lib",
            "/lib64",
            "/bin",
            "/sbin",
            "/etc/ld.so.cache",
            "/etc/ld.so.conf",
            "/etc/ld.so.conf.d",
            "/etc/ssl/certs",
            "/dev/null",
            "/dev/urandom",
            "/dev/random",
        ] {
            let path = Path::new(path);
            if path.exists() {
                runtime_paths.push(path.to_path_buf());
            }
        }
        if profile == SandboxProfile::DataSourcePlugin {
            for path in [
                "/etc/hosts",
                "/etc/resolv.conf",
                "/etc/nsswitch.conf",
                "/etc/odbc.ini",
                "/etc/odbcinst.ini",
            ] {
                let path = Path::new(path);
                if path.exists() {
                    runtime_paths.push(path.to_path_buf());
                }
            }
        }
        runtime_paths.sort();
        runtime_paths.dedup();
        for path in runtime_paths {
            if !path.exists() {
                continue;
            }
            let canonical = path
                .canonicalize()
                .map_err(|error|
                    format!(
                        "could not canonicalize Landlock runtime path {}: {error}",
                        path.display()
                    )
                )?;
            let metadata = std::fs
                ::metadata(&canonical)
                .map_err(|error|
                    format!(
                        "could not inspect Landlock runtime path {}: {error}",
                        canonical.display()
                    )
                )?;
            let mut allowed = EXECUTE | READ_FILE;
            if metadata.is_dir() {
                allowed |= READ_DIR;
            }
            add_path_rule(ruleset_fd, &canonical, allowed, LANDLOCK_RULE_PATH_BENEATH)?;
        }

        let mut read_paths = extra_read_paths.to_vec();
        read_paths.sort();
        read_paths.dedup();
        for path in read_paths {
            let canonical = path
                .canonicalize()
                .map_err(|error|
                    format!("could not canonicalize Landlock read path {}: {error}", path.display())
                )?;
            let metadata = std::fs
                ::metadata(&canonical)
                .map_err(|error|
                    format!("could not inspect Landlock read path {}: {error}", canonical.display())
                )?;
            let mut allowed = READ_FILE;
            if metadata.is_dir() {
                allowed |= READ_DIR;
            }
            add_path_rule(ruleset_fd, &canonical, allowed, LANDLOCK_RULE_PATH_BENEATH)?;
        }

        let mut library_paths = extra_library_paths.to_vec();
        library_paths.sort();
        library_paths.dedup();
        for path in library_paths {
            let canonical = path
                .canonicalize()
                .map_err(|error|
                    format!(
                        "could not canonicalize Landlock library path {}: {error}",
                        path.display()
                    )
                )?;
            let metadata = std::fs
                ::metadata(&canonical)
                .map_err(|error|
                    format!(
                        "could not inspect Landlock library path {}: {error}",
                        canonical.display()
                    )
                )?;
            let mut allowed = EXECUTE | READ_FILE;
            if metadata.is_dir() {
                allowed |= READ_DIR;
            }
            add_path_rule(ruleset_fd, &canonical, allowed, LANDLOCK_RULE_PATH_BENEATH)?;
        }

        let mut writable =
            WRITE_FILE |
            REMOVE_DIR |
            REMOVE_FILE |
            MAKE_DIR |
            MAKE_REG |
            MAKE_SOCK |
            MAKE_FIFO |
            MAKE_SYM;
        if abi >= 2 {
            writable |= REFER;
        }
        if abi >= 3 {
            writable |= TRUNCATE;
        }
        add_path_rule(
            ruleset_fd,
            temp_directory,
            EXECUTE | READ_FILE | READ_DIR | writable,
            LANDLOCK_RULE_PATH_BENEATH
        )?;

        if profile == SandboxProfile::DataSourcePlugin {
            for port in datasource_tcp_ports {
                let attr = NetPortAttr { allowed_access: NET_CONNECT_TCP, port: u64::from(*port) };
                let result = unsafe {
                    libc::syscall(
                        SYS_LANDLOCK_ADD_RULE,
                        ruleset_fd,
                        LANDLOCK_RULE_NET_PORT,
                        &attr,
                        0u32
                    )
                };
                if result != 0 {
                    return Err(
                        format!(
                            "could not add Landlock TCP port rule for {port}: {}",
                            std::io::Error::last_os_error()
                        )
                    );
                }
            }
        }
        let result = unsafe { libc::syscall(SYS_LANDLOCK_RESTRICT_SELF, ruleset_fd, 0u32) };
        if result != 0 {
            return Err(
                format!("Landlock restrict_self failed: {}", std::io::Error::last_os_error())
            );
        }
        Ok(())
    })();
    unsafe {
        libc::close(ruleset_fd);
    }
    result.map(|_| abi)
}

#[cfg(target_os = "linux")]
fn add_path_rule(
    ruleset_fd: i32,
    path: &Path,
    allowed_access: u64,
    rule_type: libc::c_uint
) -> Result<(), String> {
    use std::os::unix::ffi::OsStrExt;
    const SYS_LANDLOCK_ADD_RULE: libc::c_long = 445;
    const O_PATH_C: libc::c_int = 0x200000;
    const O_CLOEXEC_C: libc::c_int = libc::O_CLOEXEC;
    #[repr(C)]
    struct PathBeneathAttr {
        allowed_access: u64,
        parent_fd: i32,
    }
    let c_path = std::ffi::CString
        ::new(path.as_os_str().as_bytes())
        .map_err(|_| "Landlock path contains NUL")?;
    let fd = unsafe { libc::open(c_path.as_ptr(), O_PATH_C | O_CLOEXEC_C) };
    if fd < 0 {
        return Err(
            format!(
                "could not open Landlock path {}: {}",
                path.display(),
                std::io::Error::last_os_error()
            )
        );
    }
    let attr = PathBeneathAttr { allowed_access, parent_fd: fd };
    let result = unsafe {
        libc::syscall(SYS_LANDLOCK_ADD_RULE, ruleset_fd, rule_type, &attr, 0u32)
    };
    let close_result = unsafe { libc::close(fd) };
    if result != 0 {
        return Err(
            format!(
                "could not add Landlock path rule for {}: {}",
                path.display(),
                std::io::Error::last_os_error()
            )
        );
    }
    if close_result != 0 {
        return Err(
            format!(
                "could not close Landlock path descriptor for {}: {}",
                path.display(),
                std::io::Error::last_os_error()
            )
        );
    }
    Ok(())
}

#[cfg(target_os = "linux")]
fn apply_seccomp(profile: SandboxProfile) -> Result<(), String> {
    #[repr(C)]
    #[derive(Clone, Copy)]
    struct Filter {
        code: u16,
        jt: u8,
        jf: u8,
        k: u32,
    }
    #[repr(C)]
    struct Program {
        len: u16,
        filter: *const Filter,
    }

    const BPF_LD_W_ABS: u16 = 0x20;
    const BPF_JMP_JEQ_K: u16 = 0x15;
    const BPF_JMP_JSET_K: u16 = 0x45;
    const BPF_RET_K: u16 = 0x06;
    const SECCOMP_RET_KILL_PROCESS: u32 = 0x8000_0000;
    const SECCOMP_RET_ERRNO: u32 = 0x0005_0000;
    const SECCOMP_RET_ALLOW: u32 = 0x7fff_0000;
    const X32_SYSCALL_BIT: u32 = 0x4000_0000;
    const CLONE_THREAD_FLAG: u32 = 0x0001_0000;
    const SECCOMP_SET_MODE_FILTER: libc::c_int = 1;
    const SECCOMP_FILTER_FLAG_TSYNC: libc::c_uint = 1;

    #[cfg(target_arch = "x86_64")]
    const AUDIT_ARCH: u32 = 0xc000_003e;
    #[cfg(target_arch = "aarch64")]
    const AUDIT_ARCH: u32 = 0xc000_00b7;
    #[cfg(not(any(target_arch = "x86_64", target_arch = "aarch64")))]
    const AUDIT_ARCH: u32 = 0;
    #[cfg(not(any(target_arch = "x86_64", target_arch = "aarch64")))]
    return Err("seccomp enforced profile is supported only on Linux x86_64 and aarch64".into());

    let mut filters = vec![
        Filter { code: BPF_LD_W_ABS, jt: 0, jf: 0, k: 4 }, // seccomp_data.arch
        Filter { code: BPF_JMP_JEQ_K, jt: 1, jf: 0, k: AUDIT_ARCH },
        Filter { code: BPF_RET_K, jt: 0, jf: 0, k: SECCOMP_RET_KILL_PROCESS },
        Filter { code: BPF_LD_W_ABS, jt: 0, jf: 0, k: 0 } // seccomp_data.nr
    ];
    #[cfg(target_arch = "x86_64")]
    {
        filters.push(Filter { code: BPF_JMP_JSET_K, jt: 0, jf: 1, k: X32_SYSCALL_BIT });
        filters.push(Filter { code: BPF_RET_K, jt: 0, jf: 0, k: SECCOMP_RET_KILL_PROCESS });
    }

    // Processes may create threads but not fork another process. `clone3` returns ENOSYS so
    // libc can safely fall back to the classic clone syscall, whose CLONE_THREAD flag is checked.
    filters.push(Filter { code: BPF_JMP_JEQ_K, jt: 0, jf: 4, k: libc::SYS_clone as u32 });
    filters.push(Filter { code: BPF_LD_W_ABS, jt: 0, jf: 0, k: 16 }); // args[0], clone flags
    filters.push(Filter { code: BPF_JMP_JSET_K, jt: 1, jf: 0, k: CLONE_THREAD_FLAG });
    filters.push(Filter {
        code: BPF_RET_K,
        jt: 0,
        jf: 0,
        k: SECCOMP_RET_ERRNO | (libc::EPERM as u32),
    });
    filters.push(Filter { code: BPF_LD_W_ABS, jt: 0, jf: 0, k: 0 }); // restore syscall nr

    let mut denied = vec![
        // fork/vfork have dedicated syscall numbers on x86_64. On aarch64 they are implemented
        // through clone, which is guarded above by requiring CLONE_THREAD.
        (libc::SYS_clone3 as u32, libc::ENOSYS),
        (libc::SYS_execve as u32, libc::EPERM),
        (libc::SYS_execveat as u32, libc::EPERM),
        (libc::SYS_ptrace as u32, libc::EPERM),
        (libc::SYS_prctl as u32, libc::EPERM),
        (libc::SYS_process_vm_readv as u32, libc::EPERM),
        (libc::SYS_mount as u32, libc::EPERM),
        (libc::SYS_umount2 as u32, libc::EPERM),
        (libc::SYS_unshare as u32, libc::EPERM),
        (libc::SYS_setns as u32, libc::EPERM),
        (libc::SYS_bpf as u32, libc::EPERM),
        (libc::SYS_perf_event_open as u32, libc::EPERM),
        (libc::SYS_keyctl as u32, libc::EPERM),
        (libc::SYS_open_by_handle_at as u32, libc::EPERM),
        (libc::SYS_init_module as u32, libc::EPERM),
        (libc::SYS_finit_module as u32, libc::EPERM),
        (libc::SYS_delete_module as u32, libc::EPERM),
        (libc::SYS_kexec_load as u32, libc::EPERM),
        (libc::SYS_reboot as u32, libc::EPERM),
        (libc::SYS_io_uring_setup as u32, libc::EPERM),
        (libc::SYS_userfaultfd as u32, libc::EPERM),
        (libc::SYS_pidfd_getfd as u32, libc::EPERM),
        (libc::SYS_pidfd_open as u32, libc::EPERM),
        (libc::SYS_process_vm_writev as u32, libc::EPERM),
        (libc::SYS_process_madvise as u32, libc::EPERM),
        (libc::SYS_process_mrelease as u32, libc::EPERM),
        (libc::SYS_capset as u32, libc::EPERM),
        (libc::SYS_kill as u32, libc::EPERM),
        (libc::SYS_tgkill as u32, libc::EPERM),
        (libc::SYS_rt_sigqueueinfo as u32, libc::EPERM),
        (libc::SYS_rt_tgsigqueueinfo as u32, libc::EPERM),
        (libc::SYS_pidfd_send_signal as u32, libc::EPERM),
        (libc::SYS_setuid as u32, libc::EPERM),
        (libc::SYS_setgid as u32, libc::EPERM),
        (libc::SYS_setreuid as u32, libc::EPERM),
        (libc::SYS_setregid as u32, libc::EPERM),
        (libc::SYS_setresuid as u32, libc::EPERM),
        (libc::SYS_setresgid as u32, libc::EPERM),
        (libc::SYS_setfsuid as u32, libc::EPERM),
        (libc::SYS_setfsgid as u32, libc::EPERM),
        (libc::SYS_setgroups as u32, libc::EPERM),
        (libc::SYS_chroot as u32, libc::EPERM),
        (libc::SYS_pivot_root as u32, libc::EPERM),
        (libc::SYS_personality as u32, libc::EPERM),
        (libc::SYS_syslog as u32, libc::EPERM)
    ];
    #[cfg(target_arch = "x86_64")]
    denied.extend([
        (libc::SYS_fork as u32, libc::EPERM),
        (libc::SYS_vfork as u32, libc::EPERM),
        (libc::SYS_tkill as u32, libc::EPERM),
    ]);

    if profile == SandboxProfile::DomainPlugin {
        denied.extend([
            (libc::SYS_socket as u32, libc::EPERM),
            (libc::SYS_connect as u32, libc::EPERM),
            (libc::SYS_bind as u32, libc::EPERM),
            (libc::SYS_listen as u32, libc::EPERM),
            (libc::SYS_accept as u32, libc::EPERM),
            (libc::SYS_accept4 as u32, libc::EPERM),
            (libc::SYS_socketpair as u32, libc::EPERM),
            (libc::SYS_sendto as u32, libc::EPERM),
            (libc::SYS_recvfrom as u32, libc::EPERM),
            (libc::SYS_sendmsg as u32, libc::EPERM),
            (libc::SYS_recvmsg as u32, libc::EPERM),
        ]);
    } else if profile == SandboxProfile::DataSourcePlugin {
        // TCP/UDP IP sockets remain available for the database driver, but Unix-domain sockets,
        // netlink, packet sockets and other address families are rejected. Egress IP filtering is
        // an additional deployment-side requirement, not a seccomp guarantee.
        filters.extend([
            Filter { code: BPF_JMP_JEQ_K, jt: 0, jf: 4, k: libc::SYS_socket as u32 },
            Filter { code: BPF_LD_W_ABS, jt: 0, jf: 0, k: 16 }, // args[0], address family
            Filter { code: BPF_JMP_JEQ_K, jt: 2, jf: 0, k: libc::AF_INET as u32 },
            Filter { code: BPF_JMP_JEQ_K, jt: 1, jf: 0, k: libc::AF_INET6 as u32 },
            Filter { code: BPF_RET_K, jt: 0, jf: 0, k: SECCOMP_RET_ERRNO | (libc::EPERM as u32) },
            Filter { code: BPF_LD_W_ABS, jt: 0, jf: 0, k: 0 }, // restore syscall nr
        ]);
    }
    for (syscall_number, errno) in denied {
        filters.push(Filter { code: BPF_JMP_JEQ_K, jt: 0, jf: 1, k: syscall_number });
        filters.push(Filter {
            code: BPF_RET_K,
            jt: 0,
            jf: 0,
            k: SECCOMP_RET_ERRNO | (errno as u32),
        });
    }
    filters.push(Filter { code: BPF_RET_K, jt: 0, jf: 0, k: SECCOMP_RET_ALLOW });

    let program = Program { len: filters.len() as u16, filter: filters.as_ptr() };
    // SAFETY: the BPF array and program descriptor remain alive for the syscall. TSYNC ensures
    // any thread unexpectedly created before sandbox activation receives the same restrictions.
    let result = unsafe {
        libc::syscall(
            libc::SYS_seccomp,
            SECCOMP_SET_MODE_FILTER,
            SECCOMP_FILTER_FLAG_TSYNC,
            &program
        )
    };
    if result != 0 {
        return Err(
            format!("could not install seccomp filter: {}", std::io::Error::last_os_error())
        );
    }
    Ok(())
}
