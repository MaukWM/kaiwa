use futures::StreamExt;
use poise::serenity_prelude as serenity;
use songbird::events::{Event, EventContext, EventHandler as VoiceEventHandler};
use songbird::SerenityInit;
use tokio_tungstenite::tungstenite;

const OPENAI_REALTIME_URL: &str = "wss://api.openai.com/v1/realtime?model=gpt-realtime-2";

struct Data {}

type Error = Box<dyn std::error::Error + Send + Sync>;
type Context<'a> = poise::Context<'a, Data, Error>;

struct VoiceReceiver;

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
                    if let Some(_decoded) = &data.decoded_voice {
                        // Audio frames available here — Phase 3 will forward these to OpenAI.
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

async fn connect_openai() -> Result<(), Error> {
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
                        "type": "semantic_vad",
                        "eagerness": "low"
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

    Ok(())
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

    // Decode incoming audio to 24kHz mono PCM — the format OpenAI expects.
    manager.set_config(songbird::Config::default().decode_mode(
        songbird::driver::DecodeMode::Decode(songbird::driver::DecodeConfig::new(
            songbird::driver::Channels::Mono,
            songbird::driver::SampleRate::Hz24000,
        )),
    ));

    let call = manager.join(guild_id, channel_id).await?;

    {
        let mut call = call.lock().await;
        call.add_global_event(Event::Core(songbird::CoreEvent::SpeakingStateUpdate), VoiceReceiver);
        call.add_global_event(Event::Core(songbird::CoreEvent::VoiceTick), VoiceReceiver);
        call.add_global_event(Event::Core(songbird::CoreEvent::ClientDisconnect), VoiceReceiver);
    }

    connect_openai().await?;

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
