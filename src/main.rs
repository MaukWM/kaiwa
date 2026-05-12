use std::collections::VecDeque;
use std::io::{Read, Seek, SeekFrom};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

use base64::Engine;
use futures::{SinkExt, StreamExt};
use parking_lot::Mutex as SyncMutex;
use poise::serenity_prelude as serenity;
use songbird::events::{Event, EventContext, EventHandler as VoiceEventHandler};
use songbird::input::{Input, RawAdapter};
use songbird::Call;
use songbird::SerenityInit;
use tokio::sync::{mpsc, Mutex};
use tokio_tungstenite::tungstenite;

const DEFAULT_MODEL: &str = "gpt-realtime-mini";

struct Data {}

type Error = Box<dyn std::error::Error + Send + Sync>;
type Context<'a> = poise::Context<'a, Data, Error>;

struct StreamingAudioSource {
    buffer: Arc<SyncMutex<VecDeque<u8>>>,
}

impl Read for StreamingAudioSource {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        let mut ring = self.buffer.lock();
        if ring.is_empty() {
            buf.iter_mut().for_each(|b| *b = 0);
            Ok(buf.len())
        } else {
            let to_read = buf.len().min(ring.len());
            for byte in buf[..to_read].iter_mut() {
                *byte = ring.pop_front().unwrap();
            }
            Ok(to_read)
        }
    }
}

impl Seek for StreamingAudioSource {
    fn seek(&mut self, _pos: SeekFrom) -> std::io::Result<u64> {
        Err(std::io::Error::new(
            std::io::ErrorKind::Unsupported,
            "not seekable",
        ))
    }
}

impl symphonia::core::io::MediaSource for StreamingAudioSource {
    fn is_seekable(&self) -> bool {
        false
    }
    fn byte_len(&self) -> Option<u64> {
        None
    }
}

struct VoiceReceiver {
    audio_tx: Arc<mpsc::Sender<Vec<i16>>>,
    ai_speaking: Arc<AtomicBool>,
}

#[async_trait::async_trait]
impl VoiceEventHandler for VoiceReceiver {
    async fn act(&self, ctx: &EventContext<'_>) -> Option<Event> {
        match ctx {
            EventContext::SpeakingStateUpdate(speaking) => {
                tracing::debug!(
                    "User {:?} speaking state: ssrc={}",
                    speaking.user_id,
                    speaking.ssrc,
                );
            }
            EventContext::VoiceTick(tick) => {
                let ai_speaking = self.ai_speaking.load(Ordering::Relaxed);
                let mut sent = false;

                for (_ssrc, data) in &tick.speaking {
                    if let Some(decoded) = &data.decoded_voice {
                        let sum_sq: f64 =
                            decoded.iter().map(|&s| (s as f64) * (s as f64)).sum();
                        let rms = (sum_sq / decoded.len() as f64).sqrt();

                        if rms > 300.0 {
                            let _ = self.audio_tx.try_send(decoded.clone());
                        } else if !ai_speaking {
                            let _ = self.audio_tx.try_send(vec![0i16; 480]);
                        }
                        sent = true;
                    }
                }

                if !sent && !ai_speaking {
                    let _ = self.audio_tx.try_send(vec![0i16; 480]);
                }
            }
            EventContext::ClientDisconnect(disconnect) => {
                tracing::info!("User {:?} disconnected from voice", disconnect.user_id);
            }
            _ => {}
        }
        None
    }
}

fn pcm16_to_f32_bytes(pcm16: &[u8]) -> Vec<u8> {
    let mut f32_bytes = Vec::with_capacity(pcm16.len() * 2);
    for chunk in pcm16.chunks_exact(2) {
        let sample = i16::from_le_bytes([chunk[0], chunk[1]]);
        let f32_sample = sample as f32 / 32768.0;
        f32_bytes.extend_from_slice(&f32_sample.to_le_bytes());
    }
    f32_bytes
}

async fn connect_openai(
    call: Arc<Mutex<Call>>,
    ai_speaking: Arc<AtomicBool>,
) -> Result<mpsc::Sender<Vec<i16>>, Error> {
    let api_key = std::env::var("OPENAI_API_KEY").expect("OPENAI_API_KEY must be set");
    let model = std::env::var("OPENAI_MODEL").unwrap_or_else(|_| DEFAULT_MODEL.to_string());
    let url = format!("wss://api.openai.com/v1/realtime?model={model}");

    let request = tungstenite::http::Request::builder()
        .uri(&url)
        .header("Authorization", format!("Bearer {api_key}"))
        .header("Host", "api.openai.com")
        .header("Connection", "Upgrade")
        .header("Upgrade", "websocket")
        .header("Sec-WebSocket-Version", "13")
        .header(
            "Sec-WebSocket-Key",
            tungstenite::handshake::client::generate_key(),
        )
        .body(())?;

    let (ws_stream, _) =
        tokio_tungstenite::connect_async_tls_with_config(request, None, false, None).await?;

    tracing::info!("Connected to OpenAI Realtime API");

    let (mut write, mut read) = ws_stream.split();

    let session_update = serde_json::json!({
        "type": "session.update",
        "session": {
            "type": "realtime",
            "output_modalities": ["audio"],
            "audio": {
                "input": {
                    "turn_detection": {
                        "type": "server_vad",
                        "threshold": 0.5,
                        "silence_duration_ms": 500,
                        "prefix_padding_ms": 300
                    }
                },
                "output": {
                    "format": { "type": "audio/pcm", "rate": 24000 },
                    "voice": "ash"
                }
            }
        }
    });

    write
        .send(tungstenite::Message::Text(
            session_update.to_string().into(),
        ))
        .await?;

    tracing::info!("Sent session.update to OpenAI");

    let (audio_tx, mut audio_rx) = mpsc::channel::<Vec<i16>>(50);

    let playback_buffer: Arc<SyncMutex<VecDeque<u8>>> =
        Arc::new(SyncMutex::new(VecDeque::with_capacity(96000)));

    {
        let source = StreamingAudioSource {
            buffer: playback_buffer.clone(),
        };
        let raw = RawAdapter::new(source, 24000, 1);
        let input = Input::from(raw);
        let mut handler = call.lock().await;
        handler.play_input(input);
        tracing::info!("Streaming playback track started");
    }

    // Task: forward audio frames to OpenAI.
    tokio::spawn(async move {
        while let Some(samples) = audio_rx.recv().await {
            let bytes: Vec<u8> = samples.iter().flat_map(|s| s.to_le_bytes()).collect();
            let encoded = base64::engine::general_purpose::STANDARD.encode(&bytes);

            let event = serde_json::json!({
                "type": "input_audio_buffer.append",
                "audio": encoded,
            });

            if let Err(e) = write
                .send(tungstenite::Message::Text(event.to_string().into()))
                .await
            {
                tracing::error!("Failed to send audio to OpenAI: {}", e);
                break;
            }
        }
        tracing::warn!("Audio send task ended");
    });

    let playback_buf = playback_buffer.clone();

    // Task: receive events from OpenAI, push audio into the ring buffer.
    tokio::spawn(async move {
        while let Some(msg) = read.next().await {
            match msg {
                Ok(tungstenite::Message::Text(text)) => {
                    if let Ok(event) = serde_json::from_str::<serde_json::Value>(&text) {
                        let event_type = event["type"].as_str().unwrap_or("unknown");
                        match event_type {
                            "session.created" => {
                                let id = event["session"]["id"].as_str().unwrap_or("?");
                                tracing::info!("OpenAI session created: {}", id);
                            }
                            "session.updated" => {
                                tracing::info!("OpenAI session configured");
                            }
                            "input_audio_buffer.speech_started" => {
                                tracing::info!("Speech detected");
                                playback_buf.lock().clear();
                                ai_speaking.store(false, Ordering::Relaxed);
                            }
                            "input_audio_buffer.speech_stopped" => {
                                tracing::info!("Speech ended");
                            }
                            "response.created" => {
                                ai_speaking.store(true, Ordering::Relaxed);
                                tracing::info!("Streaming AI response...");
                            }
                            "response.output_audio.delta" => {
                                if let Some(delta) = event["delta"].as_str() {
                                    if let Ok(pcm16) =
                                        base64::engine::general_purpose::STANDARD.decode(delta)
                                    {
                                        let f32_data = pcm16_to_f32_bytes(&pcm16);
                                        playback_buf.lock().extend(f32_data.iter());
                                    }
                                }
                            }
                            "response.output_audio.done" => {
                                tracing::info!("Response audio stream complete");
                            }
                            "response.done" => {
                                ai_speaking.store(false, Ordering::Relaxed);
                                tracing::info!("OpenAI response complete");
                            }
                            "error" => {
                                tracing::error!("OpenAI error: {}", event["error"]);
                            }
                            _ => {
                                tracing::debug!("OpenAI event: {}", event_type);
                            }
                        }
                    }
                }
                Ok(_) => {}
                Err(e) => {
                    tracing::error!("OpenAI WebSocket error: {}", e);
                    break;
                }
            }
        }
        tracing::warn!("OpenAI WebSocket closed");
    });

    Ok(audio_tx)
}

#[poise::command(slash_command, guild_only)]
async fn join(ctx: Context<'_>) -> Result<(), Error> {
    let guild_id = ctx.guild_id().ok_or("Must be used in a server")?;

    let channel_id = {
        let guild = ctx.guild().ok_or("Could not get guild")?;
        guild
            .voice_states
            .get(&ctx.author().id)
            .and_then(|vs| vs.channel_id)
    };

    let channel_id = match channel_id {
        Some(id) => id,
        None => {
            ctx.say("Join a voice channel first.").await?;
            return Ok(());
        }
    };

    let manager = songbird::get(ctx.serenity_context())
        .await
        .ok_or("Songbird not initialized")?
        .clone();

    manager.set_config(songbird::Config::default().decode_mode(
        songbird::driver::DecodeMode::Decode(songbird::driver::DecodeConfig::new(
            songbird::driver::Channels::Mono,
            songbird::driver::SampleRate::Hz24000,
        )),
    ));

    let call = manager.join(guild_id, channel_id).await?;
    let ai_speaking = Arc::new(AtomicBool::new(false));

    let audio_tx = connect_openai(call.clone(), ai_speaking.clone()).await?;
    let audio_tx = Arc::new(audio_tx);

    {
        let mut handler = call.lock().await;
        let receiver = VoiceReceiver {
            audio_tx: audio_tx.clone(),
            ai_speaking: ai_speaking.clone(),
        };
        handler.add_global_event(
            Event::Core(songbird::CoreEvent::SpeakingStateUpdate),
            receiver,
        );
        let receiver = VoiceReceiver {
            audio_tx: audio_tx.clone(),
            ai_speaking: ai_speaking.clone(),
        };
        handler.add_global_event(Event::Core(songbird::CoreEvent::VoiceTick), receiver);
        let receiver = VoiceReceiver {
            audio_tx,
            ai_speaking,
        };
        handler.add_global_event(
            Event::Core(songbird::CoreEvent::ClientDisconnect),
            receiver,
        );
    }

    ctx.say(format!("Joined <#{channel_id}>. Use `/leave` when done."))
        .await?;
    tracing::info!("Joined voice channel {} in guild {}", channel_id, guild_id);

    Ok(())
}

#[poise::command(slash_command, guild_only)]
async fn leave(ctx: Context<'_>) -> Result<(), Error> {
    let guild_id = ctx.guild_id().ok_or("Must be used in a server")?;

    let manager = songbird::get(ctx.serenity_context())
        .await
        .ok_or("Songbird not initialized")?
        .clone();

    manager.remove(guild_id).await?;

    ctx.say("Left the voice channel.").await?;
    tracing::info!("Left voice in guild {}", guild_id);

    Ok(())
}

#[tokio::main]
async fn main() {
    let _ = dotenvy::dotenv();

    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "info,kaiwa=debug".parse().unwrap()),
        )
        .init();

    let token = std::env::var("DISCORD_TOKEN").expect("DISCORD_TOKEN must be set");

    let intents = serenity::GatewayIntents::non_privileged()
        | serenity::GatewayIntents::GUILD_VOICE_STATES;

    let framework = poise::Framework::builder()
        .options(poise::FrameworkOptions {
            commands: vec![join(), leave()],
            ..Default::default()
        })
        .setup(|ctx, _ready, framework| {
            Box::pin(async move {
                poise::builtins::register_globally(ctx, &framework.options().commands).await?;
                tracing::info!("Bot is ready!");
                Ok(Data {})
            })
        })
        .build();

    let mut client = serenity::ClientBuilder::new(&token, intents)
        .framework(framework)
        .register_songbird()
        .await
        .expect("Failed to create client");

    if let Err(e) = client.start().await {
        tracing::error!("Client error: {}", e);
    }
}
