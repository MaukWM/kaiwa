use poise::serenity_prelude as serenity;
use songbird::events::{Event, EventContext, EventHandler as VoiceEventHandler};
use songbird::SerenityInit;

struct Data {}

type Error = Box<dyn std::error::Error + Send + Sync>;

type Context<'a> = poise::Context<'a, Data, Error>;

struct VoiceReceiver;


#[async_trait::async_trait]
impl VoiceEventHandler for VoiceReceiver {
    async fn act(&self, ctx: &EventContext<'_>) -> Option<Event> {
        match ctx {
            // Fired when a user starts/stops speaking — maps their SSRC (audio stream ID) to their UserId.
            EventContext::SpeakingStateUpdate(speaking) => {
                tracing::debug!(
                    "User {:?} speaking state: ssrc={}",
                    speaking.user_id,
                    speaking.ssrc,
                );
            }

            // Fired every 20ms with decoded audio from all speaking users.
            EventContext::VoiceTick(tick) => {
                for (ssrc, data) in &tick.speaking {
                    // `decoded_voice` contains the PCM samples if decoding is enabled.
                    if let Some(decoded) = &data.decoded_voice {
                        tracing::debug!(
                            "Audio from ssrc={}: {} samples",
                            ssrc,
                            decoded.len(),
                        );
                    }
                }
            }

            // Fired when a user disconnects from the voice channel.
            EventContext::ClientDisconnect(disconnect) => {
                tracing::info!("User {:?} disconnected from voice", disconnect.user_id);
            }

            _ => {}
        }

        None
    }
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

    // Configure Songbird to decode incoming audio to 24kHz mono PCM
    // BEFORE joining — the driver uses this config when it launches.
    // 24kHz mono is exactly what OpenAI Realtime API wants.
    manager.set_config(songbird::Config::default().decode_mode(
        songbird::driver::DecodeMode::Decode(songbird::driver::DecodeConfig::new(
            songbird::driver::Channels::Mono,
            songbird::driver::SampleRate::Hz24000,
        )),
    ));

    let call = manager.join(guild_id, channel_id).await?;

    {
        let mut call = call.lock().await;

        // Register our event handler for voice events.
        call.add_global_event(Event::Core(songbird::CoreEvent::SpeakingStateUpdate), VoiceReceiver);
        call.add_global_event(Event::Core(songbird::CoreEvent::VoiceTick), VoiceReceiver);
        call.add_global_event(Event::Core(songbird::CoreEvent::ClientDisconnect), VoiceReceiver);
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
