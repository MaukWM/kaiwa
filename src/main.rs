use poise::serenity_prelude as serenity;
use songbird::SerenityInit;

struct Data {}

type Error = Box<dyn std::error::Error + Send + Sync>;

type Context<'a> = poise::Context<'a, Data, Error>;

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

    manager.join(guild_id, channel_id).await?;

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
