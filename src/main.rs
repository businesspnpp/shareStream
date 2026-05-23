// Hide the black console window in release builds. Keep it for debug so
// `cargo run` still shows logs interactively.
#![cfg_attr(not(debug_assertions), windows_subsystem = "windows")]

//! win_screen_streamer
//!
//! Headless Windows screen streamer:
//!   * No primary window. Only a system-tray icon (Start / Stop / Exit).
//!   * Capture path:  WGC -> BGRA frame -> openh264 (software H.264) -> Annex-B NALUs.
//!   * Network path:  per-frame Annex-B bytes -> tokio mpsc -> tokio-tungstenite wss:// binary frames.
//!
//! Output is raw Annex-B H.264 (no container) so the browser viewer can decode
//! every frame via WebCodecs with sub-second latency. The relay records the
//! raw NAL stream which is a fully playable .h264 file (VLC / ffmpeg).

use std::io::{Read, Write};
use std::net::{Shutdown, TcpListener, TcpStream};
use std::sync::{
    atomic::{AtomicBool, Ordering},
    Arc, Mutex,
};
use std::thread;
use std::time::Duration;

use futures_util::SinkExt;
use tokio::sync::mpsc;
use tokio_tungstenite::{connect_async, tungstenite::protocol::Message};

use tray_icon::{
    menu::{Menu, MenuEvent, MenuItem},
    Icon, TrayIconBuilder,
};
use winit::event_loop::{ControlFlow, EventLoopBuilder};

use openh264::encoder::{BitRate, Encoder, EncoderConfig, FrameRate, Profile, RateControlMode};
use openh264::formats::{BgraSliceU8, YUVBuffer};
use openh264::OpenH264API;

use windows_capture::{
    capture::GraphicsCaptureApiHandler,
    frame::Frame,
    graphics_capture_api::InternalCaptureControl,
    monitor::Monitor,
    settings::{ColorFormat, CursorCaptureSettings, DrawBorderSettings, Settings},
};

// ---- Stream profile -------------------------------------------------------

const DEFAULT_WSS: &str = "wss://sharestream-relay.onrender.com/ingest";

fn server_url() -> String {
    std::env::var("SHARESTREAM_WSS").unwrap_or_else(|_| DEFAULT_WSS.to_string())
}

const TARGET_FPS: u32 = 30;
const TARGET_BITRATE: u32 = 600_000; // ~600 kbps

// Loopback control port. Any second invocation talks to this to drive the
// already-running tray instance.
const CONTROL_ADDR: &str = "127.0.0.1:47654";

#[cfg(windows)]
fn attach_parent_console() {
    // Re-attach stdout to the parent shell so println! reaches PowerShell.
    extern "system" {
        fn AttachConsole(pid: u32) -> i32;
    }
    const ATTACH_PARENT: u32 = 0xFFFF_FFFF;
    unsafe {
        AttachConsole(ATTACH_PARENT);
    }
}

fn send_control(cmd: &str) -> std::io::Result<String> {
    let mut s = TcpStream::connect_timeout(
        &CONTROL_ADDR.parse().unwrap(),
        Duration::from_millis(500),
    )?;
    s.set_read_timeout(Some(Duration::from_secs(2)))?;
    s.write_all(cmd.trim().as_bytes())?;
    s.write_all(b"\n")?;
    let _ = s.shutdown(Shutdown::Write);
    let mut buf = Vec::with_capacity(64);
    // Ignore RSTs after a successful read — we only care about whatever the
    // server flushed before closing.
    let _ = s.read_to_end(&mut buf);
    Ok(String::from_utf8_lossy(&buf).trim().to_string())
}

// ---- Shared control state -------------------------------------------------

struct AppState {
    is_recording: Arc<AtomicBool>,
    capture_alive: Arc<AtomicBool>,
    capture_thread: Mutex<Option<thread::JoinHandle<()>>>,
}

// ---- Capture handler ------------------------------------------------------

struct CaptureEngine {
    encoder: Option<Encoder>,
    tx: mpsc::Sender<Vec<u8>>,
    alive: Arc<AtomicBool>,
}

impl GraphicsCaptureApiHandler for CaptureEngine {
    type Flags = EngineFlags;
    type Error = Box<dyn std::error::Error + Send + Sync>;

    fn new(flags: Self::Flags) -> Result<Self, Self::Error> {
        Ok(Self {
            encoder: None,
            tx: flags.tx,
            alive: flags.alive,
        })
    }

    fn on_frame_arrived(
        &mut self,
        frame: &mut Frame,
        capture_control: InternalCaptureControl,
    ) -> Result<(), Self::Error> {
        if !self.alive.load(Ordering::Relaxed) {
            capture_control.stop();
            return Ok(());
        }

        let w = frame.width();
        let h = frame.height();

        // Lazy-init the encoder once we know the monitor dimensions. Baseline
        // profile + constant-bitrate so the WebCodecs decoder string stays a
        // simple "avc1.42E01F" (Baseline 3.1).
        if self.encoder.is_none() {
            let cfg = EncoderConfig::new()
                .max_frame_rate(FrameRate::from_hz(TARGET_FPS as f32))
                .bitrate(BitRate::from_bps(TARGET_BITRATE))
                .rate_control_mode(RateControlMode::Bitrate)
                .profile(Profile::Baseline);
            let api = OpenH264API::from_source();
            self.encoder = Some(Encoder::with_api_config(api, cfg)?);
            eprintln!("[Engine] Encoder initialised for {w}x{h} @ {TARGET_FPS}fps");
        }

        let mut buf = frame.buffer()?;
        let bgra = buf.as_raw_nopadding_buffer()?;
        let bgra_src = BgraSliceU8::new(bgra, (w as usize, h as usize));
        let yuv = YUVBuffer::from_rgb_source(bgra_src);

        let bitstream = match self.encoder.as_mut().unwrap().encode(&yuv) {
            Ok(b) => b,
            Err(e) => {
                eprintln!("[Engine] encode error: {e}");
                return Ok(());
            }
        };
        let bytes = bitstream.to_vec();
        if !bytes.is_empty() {
            // try_send: drop the frame rather than balloon RAM if the
            // network task can't keep up.
            let _ = self.tx.try_send(bytes);
        }
        Ok(())
    }

    fn on_closed(&mut self) -> Result<(), Self::Error> {
        Ok(())
    }
}

struct EngineFlags {
    tx: mpsc::Sender<Vec<u8>>,
    alive: Arc<AtomicBool>,
}

// ---- Network task ---------------------------------------------------------

async fn network_pump(mut rx: mpsc::Receiver<Vec<u8>>) {
    let url = server_url();
    let (ws, _) = match connect_async(&url).await {
        Ok(v) => v,
        Err(e) => {
            eprintln!("[Network] WSS connect to {url} failed: {e}");
            return;
        }
    };
    let (mut sink, _) = futures_util::StreamExt::split(ws);
    eprintln!("[Network] Tunnel established.");

    while let Some(chunk) = rx.recv().await {
        if let Err(e) = sink.send(Message::Binary(chunk)).await {
            eprintln!("[Network] Send failure: {e}");
            break;
        }
    }
    let _ = sink.close().await;
}

// ---- Capture lifecycle ----------------------------------------------------

fn start_capture(alive: Arc<AtomicBool>, rt: tokio::runtime::Handle) -> thread::JoinHandle<()> {
    thread::spawn(move || {
        let (tx, rx) = mpsc::channel::<Vec<u8>>(32);
        rt.spawn(network_pump(rx));

        let monitor = match Monitor::primary() {
            Ok(m) => m,
            Err(e) => {
                eprintln!("[Engine] No primary monitor: {e}");
                alive.store(false, Ordering::Relaxed);
                return;
            }
        };

        let settings = Settings::new(
            monitor,
            CursorCaptureSettings::WithCursor,
            DrawBorderSettings::WithoutBorder,
            ColorFormat::Bgra8,
            EngineFlags {
                tx,
                alive: alive.clone(),
            },
        );

        if let Err(e) = CaptureEngine::start(settings) {
            eprintln!("[Engine] capture stopped: {e}");
        }
        alive.store(false, Ordering::Relaxed);
    })
}

// ---- Main: tray + Win32 message loop --------------------------------------

#[derive(Clone, Copy)]
enum Cmd {
    Start,
    Stop,
    Exit,
}

fn main() {
    let args: Vec<String> = std::env::args().skip(1).collect();
    let sub = args.first().map(|s| s.to_lowercase()).unwrap_or_default();

    // Try to claim the control port. If something else owns it, we are a
    // second invocation: forward the user's command and exit.
    let listener = match TcpListener::bind(CONTROL_ADDR) {
        Ok(l) => l,
        Err(_) => {
            #[cfg(windows)]
            attach_parent_console();
            let to_send = if sub.is_empty() { "status" } else { sub.as_str() };
            match send_control(to_send) {
                Ok(resp) => {
                    println!("{resp}");
                    std::process::exit(0);
                }
                Err(e) => {
                    eprintln!("control error: {e}");
                    std::process::exit(1);
                }
            }
        }
    };

    // We're the primary instance. If user invoked with stop/status/exit but
    // no other instance was running, report it and quit (don't pop the tray).
    if matches!(sub.as_str(), "stop" | "status" | "exit") {
        #[cfg(windows)]
        attach_parent_console();
        println!("no running instance");
        drop(listener);
        std::process::exit(2);
    }

    let auto_start = sub == "start";

    let rt = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_all()
        .build()
        .expect("tokio runtime");
    let rt_handle = rt.handle().clone();

    let state = Arc::new(AppState {
        is_recording: Arc::new(AtomicBool::new(false)),
        capture_alive: Arc::new(AtomicBool::new(false)),
        capture_thread: Mutex::new(None),
    });

    // Channel: control-listener thread -> event loop.
    let (cmd_tx, cmd_rx) = std::sync::mpsc::channel::<Cmd>();

    // Accept thread for the control port.
    {
        let tx = cmd_tx.clone();
        let recording_flag = state.is_recording.clone();
        thread::spawn(move || {
            for incoming in listener.incoming() {
                let Ok(mut s) = incoming else { continue };
                let _ = s.set_read_timeout(Some(Duration::from_secs(1)));
                let mut buf = [0u8; 32];
                let n = s.read(&mut buf).unwrap_or(0);
                let line = std::str::from_utf8(&buf[..n])
                    .unwrap_or("")
                    .trim()
                    .to_lowercase();
                let reply = match line.as_str() {
                    "start" => {
                        let _ = tx.send(Cmd::Start);
                        "ok: starting"
                    }
                    "stop" => {
                        let _ = tx.send(Cmd::Stop);
                        "ok: stopping"
                    }
                    "exit" | "quit" => {
                        let _ = tx.send(Cmd::Exit);
                        "ok: exiting"
                    }
                    "status" => {
                        if recording_flag.load(Ordering::SeqCst) {
                            "streaming"
                        } else {
                            "idle"
                        }
                    }
                    _ => "err: unknown command (start|stop|status|exit)",
                };
                let _ = s.write_all(reply.as_bytes());
                let _ = s.write_all(b"\n");
                let _ = s.flush();
                let _ = s.shutdown(Shutdown::Both);
            }
        });
    }

    let tray_menu = Menu::new();
    let start_item = MenuItem::new("Start Live Stream", true, None);
    let stop_item = MenuItem::new("Stop Stream", false, None);
    let exit_item = MenuItem::new("Exit", true, None);
    tray_menu
        .append_items(&[&start_item, &stop_item, &exit_item])
        .unwrap();

    // Solid colored 32x32 icons so the tray entry is actually visible.
    fn solid_icon(r: u8, g: u8, b: u8) -> Icon {
        let mut px = Vec::with_capacity(32 * 32 * 4);
        for _ in 0..(32 * 32) {
            px.extend_from_slice(&[r, g, b, 255]);
        }
        Icon::from_rgba(px, 32, 32).unwrap()
    }
    let idle_icon = solid_icon(0, 200, 255); // bright cyan = idle
    let live_icon = solid_icon(220, 40, 40); // red = streaming

    let tray = TrayIconBuilder::new()
        .with_menu(Box::new(tray_menu))
        .with_tooltip(format!("ShareStream — idle\nIngest: {}", server_url()))
        .with_icon(idle_icon.clone())
        .build()
        .expect("tray icon");
    let tray = Arc::new(tray);

    eprintln!("[UI] System tray ready. Ingest endpoint: {}", server_url());

    if auto_start {
        let _ = cmd_tx.send(Cmd::Start);
    }

    let event_loop = EventLoopBuilder::new().build().expect("event loop");
    let menu_rx = MenuEvent::receiver();

    let start_id = start_item.id().clone();
    let stop_id = stop_item.id().clone();
    let exit_id = exit_item.id().clone();

    // Closures to centralise start/stop/exit so menu clicks and CLI commands
    // run the exact same code paths.
    let do_start = {
        let state = state.clone();
        let tray = tray.clone();
        let live_icon = live_icon.clone();
        let rt_handle = rt_handle.clone();
        let start_item = start_item.clone();
        let stop_item = stop_item.clone();
        move || {
            if state.is_recording.load(Ordering::SeqCst) {
                return;
            }
            eprintln!("[CTL] start");
            state.is_recording.store(true, Ordering::SeqCst);
            state.capture_alive.store(true, Ordering::Relaxed);
            start_item.set_enabled(false);
            stop_item.set_enabled(true);
            let _ = tray.set_icon(Some(live_icon.clone()));
            let _ = tray.set_tooltip(Some(format!(
                "ShareStream — LIVE\nIngest: {}",
                server_url()
            )));
            let h = start_capture(state.capture_alive.clone(), rt_handle.clone());
            *state.capture_thread.lock().unwrap() = Some(h);
        }
    };
    let do_stop = {
        let state = state.clone();
        let tray = tray.clone();
        let idle_icon = idle_icon.clone();
        let start_item = start_item.clone();
        let stop_item = stop_item.clone();
        move || {
            if !state.is_recording.load(Ordering::SeqCst) {
                return;
            }
            eprintln!("[CTL] stop");
            state.capture_alive.store(false, Ordering::Relaxed);
            if let Some(h) = state.capture_thread.lock().unwrap().take() {
                let _ = h.join();
            }
            state.is_recording.store(false, Ordering::SeqCst);
            start_item.set_enabled(true);
            stop_item.set_enabled(false);
            let _ = tray.set_icon(Some(idle_icon.clone()));
            let _ = tray.set_tooltip(Some(format!(
                "ShareStream — idle\nIngest: {}",
                server_url()
            )));
        }
    };

    event_loop
        .run(move |_event, target| {
            // Short timed wait so we can poll the CLI control channel without
            // burning CPU. ~0% idle on modern hardware.
            target.set_control_flow(ControlFlow::wait_duration(Duration::from_millis(100)));

            while let Ok(ev) = menu_rx.try_recv() {
                if ev.id == start_id {
                    do_start();
                } else if ev.id == stop_id {
                    do_stop();
                } else if ev.id == exit_id {
                    do_stop();
                    target.exit();
                }
            }

            while let Ok(cmd) = cmd_rx.try_recv() {
                match cmd {
                    Cmd::Start => do_start(),
                    Cmd::Stop => do_stop(),
                    Cmd::Exit => {
                        do_stop();
                        target.exit();
                    }
                }
            }
        })
        .unwrap();
}
