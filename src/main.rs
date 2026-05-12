use std::sync::Arc;

use base64::Engine;
use futures::StreamExt;
use poise::serenity_prelude as serenity;
use songbird::events::{Event, EventContext, EventHandler as VoiceEventHandler};
use songbird::input::{Input, RawAdapter};
use songbird::Call;
use songbird::SerenityInit;
use tokio::sync::{mpsc, Mutex};
use tokio_tungstenite::tungstenite;

const OPENAI_REALTIME_URL: &str = "wss://api.openai.com/v1/realtime?model=gpt-realtime-2";

struct Data {}

type Error = Box<dyn std::error::Error + Send + Sync>;
type Context<'a> = poise::Context<'a, Data, Error>;

struct VoiceReceiver {
    audio_tx: Arc<mpsc::Sender<Vec<i16>>>,
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
                let mut sent = false;
                for (_ssrc, data) in &tick.speaking {
                    if let Some(decoded) = &data.decoded_voice {
                        let sum_sq: f64 = decoded
                            .iter()
                            .map(|&s| (s as f64) * (s as f64))
                            .sum();
                        let rms = (sum_sq / decoded.len() as f64).sqrt();

                        if rms > 300.0 {
                            let _ = self.audio_tx.try_send(decoded.clone());
                        } else {
                            let _ = self.audio_tx.try_send(vec![0i16; 480]);
                        }
                        sent = true;
                    }
                }
                // When nobody is speaking, still send silence so OpenAI's
                // VAD can detect that speech has ended.
                if !sent {
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

/// Convert PCM16 LE bytes to f32 bytes (what songbird's RawAdapter expects).
fn pcm16_to_f32_bytes(pcm16: &[u8]) -> Vec<u8> {
    let mut f32_bytes = Vec::with_capacity(pcm16.len() * 2); // i16 = 2 bytes, f32 = 4 bytes
    for chunk in pcm16.chunks_exact(2) {
        let sample = i16::from_le_bytes([chunk[0], chunk[1]]);
        let f32_sample = sample as f32 / 32768.0;
        f32_bytes.extend_from_slice(&f32_sample.to_le_bytes());
    }
    f32_bytes
}

async fn connect_openai(call: Arc<Mutex<Call>>) -> Result<mpsc::Sender<Vec<i16>>, Error> {
    let api_key = std::env::var("OPENAI_API_KEY").expect("OPENAI_API_KEY must be set");

    let request = tungstenite::http::Request::builder()
        .uri(OPENAI_REALTIME_URL)
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
            "audio": {
                "input": {
                    "turn_detection": {
                        "type": "server_vad",
                        "threshold": 0.5,
                        "silence_duration_ms": 500,
                        "prefix_padding_ms": 300,
                        "interrupt_response": false
                    }
                }
            }
        }
    });

    use futures::SinkExt;
    write
        .send(tungstenite::Message::Text(
            session_update.to_string().into(),
        ))
        .await?;

    tracing::info!("Sent session.update to OpenAI");

    let (audio_tx, mut audio_rx) = mpsc::channel::<Vec<i16>>(50);

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

    // Task: receive events from OpenAI, buffer response audio, play it back.
    tokio::spawn(async move {
        let mut audio_buffer: Vec<u8> = Vec::new();

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
                            }
                            "input_audio_buffer.speech_stopped" => {
                                tracing::info!("Speech ended");
                            }
                            "response.created" => {
                                audio_buffer.clear();
                                tracing::info!("OpenAI generating response...");
                            }
                            "response.output_audio.delta" => {
                                if let Some(delta) = event["delta"].as_str() {
                                    if let Ok(pcm) =
                                        base64::engine::general_purpose::STANDARD.decode(delta)
                                    {
                                        audio_buffer.extend_from_slice(&pcm);
                                    }
                                }
                            }
                            "response.output_audio.done" => {
                                if !audio_buffer.is_empty() {
                                    tracing::info!(
                                        "Playing response audio ({} bytes PCM16)",
                                        audio_buffer.len()
                                    );
                                    // Convert PCM16 to f32, wrap in RawAdapter for songbird.
                                    let f32_data = pcm16_to_f32_bytes(&audio_buffer);
                                    let cursor = std::io::Cursor::new(f32_data);
                                    let raw = RawAdapter::new(cursor, 24000, 1);
                                    let input = Input::from(raw);
                                    let mut handler = call.lock().await;
                                    handler.play_input(input);
                                    audio_buffer.clear();
                                }
                            }
                            "response.done" => {
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

    let audio_tx = connect_openai(call.clone()).await?;
    let audio_tx = Arc::new(audio_tx);

    {
        let mut handler = call.lock().await;
        let receiver = VoiceReceiver {
            audio_tx: audio_tx.clone(),
        };
        handler.add_global_event(
            Event::Core(songbird::CoreEvent::SpeakingStateUpdate),
            receiver,
        );
        let receiver = VoiceReceiver {
            audio_tx: audio_tx.clone(),
        };
        handler.add_global_event(Event::Core(songbird::CoreEvent::VoiceTick), receiver);
        let receiver = VoiceReceiver { audio_tx };
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
