//! win_screen_streamer
//!
//! Headless Windows screen streamer:
//!   * No primary window. Only a system-tray icon (Start / Stop / Exit).
//!   * Capture path:  WGC -> D3D11 texture (VRAM) -> MF H.264 encoder (GPU) -> InMemoryRandomAccessStream.
//!   * Network path:  drain stream chunks -> tokio mpsc -> tokio-tungstenite wss:// binary frames.
//!
//! The encoder is configured for 854x480 @ 30 fps, ~600 kbps. Because the WGC
//! frame surface lives entirely in VRAM as an `ID3D11Texture2D`, the downscale
//! to 480p is performed on the GPU by the Media Foundation H.264 MFT (the
//! encoder accepts the full-resolution texture and is told to emit 854x480
//! NV12 internally), so system RAM never sees an uncompressed full-screen
//! frame buffer.

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

use windows::Storage::Streams::{
    Buffer, DataReader, InMemoryRandomAccessStream, InputStreamOptions,
};

use windows_capture::{
    capture::{Context, GraphicsCaptureApiHandler},
    encoder::{
        AudioSettingsBuilder, ContainerSettingsBuilder, VideoEncoder, VideoEncoderQuality,
        VideoEncoderType, VideoSettingsBuilder,
    },
    frame::Frame,
    graphics_capture_api::InternalCaptureControl,
    monitor::Monitor,
    settings::{ColorFormat, CursorCaptureSettings, DrawBorderSettings, Settings},
};

// ---- Stream profile -------------------------------------------------------

// Default ingest URL. Override at launch with the SHARESTREAM_WSS env var, e.g.
//   setx SHARESTREAM_WSS "wss://your-app.onrender.com/ingest"
const DEFAULT_WSS: &str = "wss://my-screen-streamer.onrender.com/ingest";

fn server_url() -> String {
    std::env::var("SHARESTREAM_WSS").unwrap_or_else(|_| DEFAULT_WSS.to_string())
}

const TARGET_WIDTH: u32 = 854;
const TARGET_HEIGHT: u32 = 480;
const TARGET_FPS: u32 = 30;
const TARGET_BITRATE: u32 = 600_000; // ~600 kbps
const NETWORK_CHUNK: u32 = 16 * 1024; // bytes per WSS binary frame

// ---- Shared control state -------------------------------------------------

/// Flags shared between the Win32 message loop (tray) and the capture thread.
struct AppState {
    is_recording: Arc<AtomicBool>,
    /// Set to `false` to ask the capture thread to tear down.
    capture_alive: Arc<AtomicBool>,
    /// Join handle for the capture OS thread (so Exit can wait on it).
    capture_thread: Mutex<Option<thread::JoinHandle<()>>>,
}

// ---- Capture handler ------------------------------------------------------

/// Holds the GPU-side H.264 encoder. Each WGC frame (a D3D11 texture in VRAM)
/// is handed to the encoder; the encoder writes Annex-B / fMP4 bytes into the
/// shared `InMemoryRandomAccessStream`. A separate thread drains that stream
/// and forwards chunks to the async network task.
struct CaptureEngine {
    encoder: Option<VideoEncoder>,
    alive: Arc<AtomicBool>,
}

impl GraphicsCaptureApiHandler for CaptureEngine {
    /// `Flags` is what `Settings::new` passes through to `new()`.
    type Flags = EngineFlags;
    type Error = Box<dyn std::error::Error + Send + Sync>;

    fn new(ctx: Context<Self::Flags>) -> Result<Self, Self::Error> {
        // --- GPU TEXTURE PIPELINE -------------------------------------------------
        // VideoEncoder wraps a Media Foundation Sink Writer. We request the H.264
        // hardware MFT and tell it the *output* resolution is 854x480; MF inserts
        // a GPU video processor that downscales the incoming full-screen D3D11
        // texture on the graphics card. We never copy a full-res frame to RAM.
        let encoder = VideoEncoder::new_from_stream(
            VideoSettingsBuilder::new(TARGET_WIDTH, TARGET_HEIGHT)
                .frame_rate(TARGET_FPS)
                .bitrate(TARGET_BITRATE)
                .sub_type(VideoEncoderType::H264)
                .quality(VideoEncoderQuality::Auto),
            AudioSettingsBuilder::default().disabled(true),
            ContainerSettingsBuilder::default(),
            &ctx.flags.stream,
        )?;

        Ok(Self {
            encoder: Some(encoder),
            alive: ctx.flags.alive,
        })
    }

    fn on_frame_arrived(
        &mut self,
        frame: &mut Frame,
        capture_control: InternalCaptureControl,
    ) -> Result<(), Self::Error> {
        if !self.alive.load(Ordering::Relaxed) {
            // User asked us to stop; flush + close from the WGC thread.
            if let Some(enc) = self.encoder.take() {
                let _ = enc.finish();
            }
            capture_control.stop();
            return Ok(());
        }

        // `frame` is an ID3D11Texture2D in VRAM. send_frame hands the GPU
        // resource straight to the MF encoder MFT - no CPU readback.
        if let Some(enc) = self.encoder.as_mut() {
            enc.send_frame(frame)?;
        }
        Ok(())
    }

    fn on_closed(&mut self) -> Result<(), Self::Error> {
        if let Some(enc) = self.encoder.take() {
            let _ = enc.finish();
        }
        Ok(())
    }
}

/// Bundle passed from the spawning thread into `CaptureEngine::new`.
struct EngineFlags {
    stream: InMemoryRandomAccessStream,
    alive: Arc<AtomicBool>,
}

// ---- Stream drain (encoder bytes -> async mpsc) ---------------------------

/// Pulls encoded bytes out of the WinRT in-memory stream and pushes them
/// onto the Tokio channel for the network task. Runs on a dedicated OS
/// thread so the WGC encoder thread is never blocked by network back-pressure.
fn drain_stream_into(
    stream: InMemoryRandomAccessStream,
    tx: mpsc::Sender<Vec<u8>>,
    alive: Arc<AtomicBool>,
) -> windows::core::Result<()> {
    let input = stream.GetInputStreamAt(0)?;
    let reader = DataReader::CreateDataReader(&input)?;
    reader.SetInputStreamOptions(InputStreamOptions::Partial)?;

    let buffer = Buffer::Create(NETWORK_CHUNK)?;
    while alive.load(Ordering::Relaxed) {
        let loaded = reader.LoadAsync(NETWORK_CHUNK)?.get()?;
        if loaded == 0 {
            // Encoder hasn't produced new bytes yet; yield briefly.
            thread::sleep(Duration::from_millis(4));
            continue;
        }
        let _ = reader.ReadBuffer(&buffer)?;
        let len = buffer.Length()? as usize;
        let mut chunk = vec![0u8; len];
        let view = DataReader::FromBuffer(&buffer)?;
        view.ReadBytes(&mut chunk)?;

        // try_send: if the network is congested we drop rather than balloon RAM.
        let _ = tx.try_send(chunk);
    }
    Ok(())
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
    println!("[Network] Tunnel established.");

    while let Some(chunk) = rx.recv().await {
        if let Err(e) = sink.send(Message::Binary(chunk)).await {
            eprintln!("[Network] Send failure: {e}");
            break;
        }
    }
    let _ = sink.close().await;
}

// ---- Capture lifecycle ----------------------------------------------------

/// Spins up the WGC capture, the drain thread, and the async WSS pump.
/// Returns the OS thread handle for the WGC loop so the UI can join on Exit.
fn start_capture(alive: Arc<AtomicBool>, rt: tokio::runtime::Handle) -> thread::JoinHandle<()> {
    thread::spawn(move || {
        // Shared encoder->network buffer (lives entirely in WinRT-managed memory).
        let stream = match InMemoryRandomAccessStream::new() {
            Ok(s) => s,
            Err(e) => {
                eprintln!("[Engine] InMemoryRandomAccessStream: {e}");
                return;
            }
        };

        let (tx, rx) = mpsc::channel::<Vec<u8>>(32);

        // Async WSS pump.
        rt.spawn(network_pump(rx));

        // Drain thread: WinRT stream -> mpsc.
        let drain_alive = alive.clone();
        let drain_stream = stream.clone();
        let drain_handle = thread::spawn(move || {
            if let Err(e) = drain_stream_into(drain_stream, tx, drain_alive) {
                eprintln!("[Engine] drain error: {e:?}");
            }
        });

        // WGC settings: primary monitor, cursor on, no yellow border.
        let monitor = match Monitor::primary() {
            Ok(m) => m,
            Err(e) => {
                eprintln!("[Engine] No primary monitor: {e}");
                alive.store(false, Ordering::Relaxed);
                let _ = drain_handle.join();
                return;
            }
        };

        let settings = Settings::new(
            monitor,
            CursorCaptureSettings::WithCursor,
            DrawBorderSettings::WithoutBorder,
            ColorFormat::Rgba8,
            EngineFlags {
                stream,
                alive: alive.clone(),
            },
        );

        // Blocking call: runs the WGC message loop on this OS thread until
        // on_frame_arrived calls capture_control.stop() (see alive flag).
        if let Err(e) = CaptureEngine::start(settings) {
            eprintln!("[Engine] capture stopped: {e}");
        }

        alive.store(false, Ordering::Relaxed);
        let _ = drain_handle.join();
    })
}

// ---- Main: tray + Win32 message loop --------------------------------------

fn main() {
    // Multi-thread tokio runtime owned by main; the UI thread itself stays
    // on a vanilla Win32 message loop (cheapest possible idle cost).
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

    // Tray menu.
    let tray_menu = Menu::new();
    let start_item = MenuItem::new("Start Live Stream", true, None);
    let stop_item = MenuItem::new("Stop Stream", false, None);
    let exit_item = MenuItem::new("Exit", true, None);
    tray_menu
        .append_items(&[&start_item, &stop_item, &exit_item])
        .unwrap();

    // 16x16 transparent placeholder icon (swap for a real .ico via include_bytes!).
    let icon_rgba = vec![0u8; 16 * 16 * 4];
    let _tray = TrayIconBuilder::new()
        .with_menu(Box::new(tray_menu))
        .with_tooltip("Rust Screen Streamer (Low Footprint)")
        .with_icon(Icon::from_rgba(icon_rgba, 16, 16).unwrap())
        .build()
        .expect("tray icon");

    println!("[UI] System tray ready.");

    // Native Win32 event loop. ControlFlow::Wait => the thread blocks in
    // GetMessage and consumes ~0% CPU while idle.
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
                    println!("[UI] Start clicked.");
                    state.is_recording.store(true, Ordering::SeqCst);
                    state.capture_alive.store(true, Ordering::Relaxed);
                    start_item.set_enabled(false);
                    stop_item.set_enabled(true);

                    let h = start_capture(state.capture_alive.clone(), rt_handle.clone());
                    *state.capture_thread.lock().unwrap() = Some(h);
                } else if ev.id == stop_id && state.is_recording.load(Ordering::SeqCst) {
                    println!("[UI] Stop clicked.");
                    state.capture_alive.store(false, Ordering::Relaxed);
                    if let Some(h) = state.capture_thread.lock().unwrap().take() {
                        let _ = h.join();
                    }
                    state.is_recording.store(false, Ordering::SeqCst);
                    start_item.set_enabled(true);
                    stop_item.set_enabled(false);
                } else if ev.id == exit_id {
                    println!("[UI] Exit clicked.");
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
