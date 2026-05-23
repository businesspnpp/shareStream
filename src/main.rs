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

use std::sync::{
    atomic::{AtomicBool, Ordering},
    Arc, Mutex,
};
use std::thread;

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

fn main() {
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
    let idle_icon = solid_icon(80, 80, 80); // gray = idle
    let live_icon = solid_icon(220, 40, 40); // red = streaming

    let tray = TrayIconBuilder::new()
        .with_menu(Box::new(tray_menu))
        .with_tooltip(format!("ShareStream — idle\nIngest: {}", server_url()))
        .with_icon(idle_icon.clone())
        .build()
        .expect("tray icon");
    let tray = Arc::new(tray);

    eprintln!("[UI] System tray ready. Ingest endpoint: {}", server_url());

    let event_loop = EventLoopBuilder::new().build().expect("event loop");
    let menu_rx = MenuEvent::receiver();

    let start_id = start_item.id().clone();
    let stop_id = stop_item.id().clone();
    let exit_id = exit_item.id().clone();

    event_loop
        .run(move |_event, target| {
            target.set_control_flow(ControlFlow::Wait);

            while let Ok(ev) = menu_rx.try_recv() {
                if ev.id == start_id && !state.is_recording.load(Ordering::SeqCst) {
                    eprintln!("[UI] Start clicked.");
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
                } else if ev.id == stop_id && state.is_recording.load(Ordering::SeqCst) {
                    eprintln!("[UI] Stop clicked.");
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
                } else if ev.id == exit_id {
                    eprintln!("[UI] Exit clicked.");
                    state.capture_alive.store(false, Ordering::Relaxed);
                    if let Some(h) = state.capture_thread.lock().unwrap().take() {
                        let _ = h.join();
                    }
                    target.exit();
                }
            }
        })
        .unwrap();
}
