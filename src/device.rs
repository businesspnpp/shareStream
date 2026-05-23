//! Collects machine identity + live telemetry into a single JSON blob that
//! the desktop client uploads to the relay on every connect. The relay
//! advertises this to the browser viewer so the user can pick which machine
//! to watch.

use serde::Serialize;
use sha2::{Digest, Sha256};
use sysinfo::{Disks, System};

#[derive(Serialize)]
pub struct NetIface {
    pub name: String,
    pub mac: Option<String>,
    pub ipv4: Vec<String>,
}

#[derive(Serialize)]
pub struct Battery {
    pub present: bool,
    pub charging: bool,
    pub on_ac: bool,
    pub percent: Option<u8>,
    pub seconds_remaining: Option<u32>,
}

#[derive(Serialize)]
pub struct DeviceInfo {
    pub device_id: String,
    pub hostname: String,
    pub username: String,
    pub os_name: String,
    pub os_version: String,
    pub kernel: String,
    pub arch: String,
    pub cpu_brand: String,
    pub cpu_cores_physical: usize,
    pub cpu_cores_logical: usize,
    pub total_memory_mb: u64,
    pub used_memory_mb: u64,
    pub disks: Vec<DiskEntry>,
    pub interfaces: Vec<NetIface>,
    pub primary_ipv4: Option<String>,
    pub battery: Battery,
    pub uptime_seconds: u64,
    pub agent_version: &'static str,
}

#[derive(Serialize)]
pub struct DiskEntry {
    pub name: String,
    pub total_gb: u64,
    pub available_gb: u64,
}

#[cfg(windows)]
#[repr(C)]
struct SystemPowerStatus {
    ac_line_status: u8,
    battery_flag: u8,
    battery_life_percent: u8,
    system_status_flag: u8,
    battery_life_time: u32,
    battery_full_life_time: u32,
}

#[cfg(windows)]
fn read_battery() -> Battery {
    extern "system" {
        fn GetSystemPowerStatus(s: *mut SystemPowerStatus) -> i32;
    }
    let mut st = SystemPowerStatus {
        ac_line_status: 255,
        battery_flag: 255,
        battery_life_percent: 255,
        system_status_flag: 0,
        battery_life_time: u32::MAX,
        battery_full_life_time: u32::MAX,
    };
    let ok = unsafe { GetSystemPowerStatus(&mut st) } != 0;
    if !ok {
        return Battery {
            present: false,
            charging: false,
            on_ac: false,
            percent: None,
            seconds_remaining: None,
        };
    }
    // battery_flag 128 = no system battery.
    let present = st.battery_flag != 128 && st.battery_flag != 255;
    let on_ac = st.ac_line_status == 1;
    let charging = (st.battery_flag & 0x08) != 0;
    let percent = if st.battery_life_percent == 255 {
        None
    } else {
        Some(st.battery_life_percent)
    };
    let seconds_remaining = if st.battery_life_time == u32::MAX {
        None
    } else {
        Some(st.battery_life_time)
    };
    Battery {
        present,
        charging,
        on_ac,
        percent,
        seconds_remaining,
    }
}

#[cfg(not(windows))]
fn read_battery() -> Battery {
    Battery {
        present: false,
        charging: false,
        on_ac: false,
        percent: None,
        seconds_remaining: None,
    }
}

fn collect_interfaces() -> (Vec<NetIface>, Option<String>) {
    let mut out: Vec<NetIface> = Vec::new();
    let primary = local_ip_address::local_ip().ok().map(|i| i.to_string());

    if let Ok(list) = local_ip_address::list_afinet_netifas() {
        // Group ipv4s by interface name.
        use std::collections::BTreeMap;
        let mut by_name: BTreeMap<String, Vec<String>> = BTreeMap::new();
        for (name, ip) in list {
            if ip.is_ipv4() {
                by_name.entry(name).or_default().push(ip.to_string());
            }
        }
        for (name, ipv4) in by_name {
            out.push(NetIface {
                name,
                mac: None,
                ipv4,
            });
        }
    }

    // Best-effort: attach the primary MAC to the first interface we found.
    if let Ok(Some(mac)) = mac_address::get_mac_address() {
        let mac_str = mac.to_string();
        if let Some(first) = out.first_mut() {
            first.mac = Some(mac_str);
        } else {
            out.push(NetIface {
                name: "primary".into(),
                mac: Some(mac_str),
                ipv4: vec![],
            });
        }
    }

    (out, primary)
}

fn stable_device_id(hostname: &str, mac: Option<&str>) -> String {
    let seed = format!("{}|{}", hostname, mac.unwrap_or("nomac"));
    let mut h = Sha256::new();
    h.update(seed.as_bytes());
    let digest = h.finalize();
    hex_lower(&digest[..6]) // 12-hex-char short id
}

fn hex_lower(bytes: &[u8]) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut s = String::with_capacity(bytes.len() * 2);
    for b in bytes {
        s.push(HEX[(b >> 4) as usize] as char);
        s.push(HEX[(b & 0x0f) as usize] as char);
    }
    s
}

pub fn collect() -> DeviceInfo {
    let mut sys = System::new();
    sys.refresh_all();

    let hostname = System::host_name().unwrap_or_else(|| "unknown-host".into());
    let username = std::env::var("USERNAME")
        .or_else(|_| std::env::var("USER"))
        .unwrap_or_else(|_| "unknown".into());
    let os_name = System::name().unwrap_or_else(|| "unknown-os".into());
    let os_version = System::os_version().unwrap_or_else(|| "?".into());
    let kernel = System::kernel_version().unwrap_or_else(|| "?".into());
    let arch = std::env::consts::ARCH.to_string();

    let cpu_brand = sys
        .cpus()
        .first()
        .map(|c| c.brand().to_string())
        .unwrap_or_else(|| "unknown-cpu".into());
    let cpu_cores_logical = sys.cpus().len();
    let cpu_cores_physical = sys.physical_core_count().unwrap_or(cpu_cores_logical);

    let total_memory_mb = sys.total_memory() / 1024 / 1024;
    let used_memory_mb = sys.used_memory() / 1024 / 1024;

    let disks_list = Disks::new_with_refreshed_list();
    let disks = disks_list
        .iter()
        .map(|d| DiskEntry {
            name: d.name().to_string_lossy().to_string(),
            total_gb: d.total_space() / 1024 / 1024 / 1024,
            available_gb: d.available_space() / 1024 / 1024 / 1024,
        })
        .collect();

    let (interfaces, primary_ipv4) = collect_interfaces();
    let primary_mac = interfaces.iter().find_map(|i| i.mac.clone());
    let device_id = stable_device_id(&hostname, primary_mac.as_deref());

    DeviceInfo {
        device_id,
        hostname,
        username,
        os_name,
        os_version,
        kernel,
        arch,
        cpu_brand,
        cpu_cores_physical,
        cpu_cores_logical,
        total_memory_mb,
        used_memory_mb,
        disks,
        interfaces,
        primary_ipv4,
        battery: read_battery(),
        uptime_seconds: System::uptime(),
        agent_version: env!("CARGO_PKG_VERSION"),
    }
}
