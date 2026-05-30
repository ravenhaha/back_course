from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=None, extra="ignore")

    database_url: str = "postgresql+asyncpg://postgres:postgres@localhost:5432/quiz"

    # Opaque (non-JWT) tokens. Access token is verified by DB lookup.
    access_token_ttl_seconds: int = 15 * 60
    refresh_token_ttl_seconds: int = 7 * 24 * 60 * 60

    # Redis is used for Pub/Sub and game session state
    redis_host: str = "localhost"
    redis_port: int = 6379


settings = Settings()
