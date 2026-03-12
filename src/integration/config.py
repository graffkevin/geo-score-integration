from pydantic_settings import BaseSettings


class Settings(BaseSettings):
    database_url: str = "postgresql://geo:geo@localhost:5432/geo_score"

    model_config = {"env_file": ".env", "extra": "ignore"}


settings = Settings()
