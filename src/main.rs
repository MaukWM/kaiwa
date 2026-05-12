use std::sync::Arc;

use base64::Engine;
use futures::StreamExt;
use poise::serenity_prelude as serenity;
use songbird::events::{Event, EventContext, EventHandler as VoiceEventHandler};
use songbird::SerenityInit;
use tokio::sync::mpsc;
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
                for (_ssrc, data) in &tick.speaking {
                    if let Some(decoded) = &data.decoded_voice {
                        // Client-side noise gate: only send frames with actual speech.
                        let sum_sq: f64 = decoded.iter()
                            .map(|&s| (s as f64) * (s as f64))
                            .sum();
                        let rms = (sum_sq / decoded.len() as f64).sqrt();

                        if rms > 300.0 {
                            let _ = self.audio_tx.try_send(decoded.clone());
                        } else {
                            // Send silence so OpenAI's VAD can detect end of speech.
                            let _ = self.audio_tx.try_send(vec![0i16; decoded.len()]);
                        }
                    }
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

async fn connect_openai() -> Result<mpsc::Sender<Vec<i16>>, Error> {
    let api_key = std::env::var("OPENAI_API_KEY").expect("OPENAI_API_KEY must be set");

    let request = tungstenite::http::Request::builder()
        .uri(OPENAI_REALTIME_URL)
        .header("Authorization", format!("Bearer {api_key}"))
        .header("Host", "api.openai.com")
        .header("Connection", "Upgrade")
        .header("Upgrade", "websocket")
        .header("Sec-WebSocket-Version", "13")
        .header("Sec-WebSocket-Key", tungstenite::handshake::client::generate_key())
        .body(())?;

    let (ws_stream, _) = tokio_tungstenite::connect_async_tls_with_config(
        request, None, false, None,
    )
    .await?;

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
                        "prefix_padding_ms": 300
                    }
                }
            }
        }
    });

    use futures::SinkExt;
    write
        .send(tungstenite::Message::Text(session_update.to_string().into()))
        .await?;

    tracing::info!("Sent session.update to OpenAI");

    let (audio_tx, mut audio_rx) = mpsc::channel::<Vec<i16>>(50);

    // Task: forward audio frames to OpenAI.
    tokio::spawn(async move {
        while let Some(samples) = audio_rx.recv().await {
            let bytes: Vec<u8> = samples
                .iter()
                .flat_map(|s| s.to_le_bytes())
                .collect();
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

    // Task: receive and log events from OpenAI.
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
                            }
                            "input_audio_buffer.speech_stopped" => {
                                tracing::info!("Speech ended");
                            }
                            "response.created" => {
                                tracing::info!("OpenAI generating response...");
                            }
                            "response.audio.delta" => {
                                tracing::debug!("OpenAI audio delta received");
                            }
                            "response.audio.done" => {
                                tracing::info!("OpenAI response audio complete");
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

    let audio_tx = connect_openai().await?;
    let audio_tx = Arc::new(audio_tx);

    {
        let mut call = call.lock().await;
        let receiver = VoiceReceiver { audio_tx: audio_tx.clone() };
        call.add_global_event(Event::Core(songbird::CoreEvent::SpeakingStateUpdate), receiver);
        let receiver = VoiceReceiver { audio_tx: audio_tx.clone() };
        call.add_global_event(Event::Core(songbird::CoreEvent::VoiceTick), receiver);
        let receiver = VoiceReceiver { audio_tx };
        call.add_global_event(Event::Core(songbird::CoreEvent::ClientDisconnect), receiver);
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
