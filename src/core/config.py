from typing import Any, Dict, Optional

import boto3
from pydantic import BaseSettings, PostgresDsn, validator


class SyncPostgresDsn(PostgresDsn):
    allowed_schemes = {"postgresql", "postgresql+psycopg2", "postgresql+pg8000"}

class Settings(BaseSettings):
    # Local Database Settings
    POSTGRES_USER: Optional[str] = None
    POSTGRES_PASSWORD: Optional[str] = None
    POSTGRES_HOST: Optional[str] = None
    POSTGRES_DB: Optional[str] = None
    POSTGRES_PORT: Optional[str] = None
    LOCAL_DATABASE_URI: Optional[SyncPostgresDsn] = None
    @validator("LOCAL_DATABASE_URI", pre=True)
    def assemble_local_db_connection(cls, v: Optional[str], values: Dict[str, Any]) -> Any:
        if isinstance(v, str):
            return v
        return SyncPostgresDsn.build(
            scheme="postgresql",
            user=values.get("POSTGRES_USER"),
            password=values.get("POSTGRES_PASSWORD"),
            host=values.get("POSTGRES_HOST"),
            path=f"/{values.get('POSTGRES_DB') or ''}",
            port=values.get("POSTGRES_PORT")
        )

    # Remote Database Settings
    POSTGRES_USER_RD: Optional[str] = None
    POSTGRES_PASSWORD_RD: Optional[str] = None
    POSTGRES_HOST_RD: Optional[str] = None
    POSTGRES_DB_RD: Optional[str] = None
    POSTGRES_PORT_RD: Optional[str] = None
    REMOTE_DATABASE_URI: Optional[SyncPostgresDsn] = None
    @validator("REMOTE_DATABASE_URI", pre=True)
    def assemble_remote_db_connection(cls, v: Optional[str], values: Dict[str, Any]) -> Any:
        if isinstance(v, str):
            return v
        return SyncPostgresDsn.build(
            scheme="postgresql",
            user=values.get("POSTGRES_USER_RD"),
            password=values.get("POSTGRES_PASSWORD_RD"),
            host=values.get("POSTGRES_HOST_RD"),
            path=f"/{values.get('POSTGRES_DB_RD') or ''}",
            port=values.get("POSTGRES_PORT_RD")
        )
    
    # CityGML Settings
    POSTGRES_USER_3DCITY: Optional[str] = None
    POSTGRES_PASSWORD_3DCITY: Optional[str] = None
    POSTGRES_HOST_3DCITY: Optional[str] = None
    POSTGRES_DB_3DCITY: Optional[str] = None
    POSTGRES_POST_3DCITY: Optional[str] = None
    CITYGML_DATABASE_URI: Optional[SyncPostgresDsn] = None
    @validator("CITYGML_DATABASE_URI", pre=True)
    def assemble_citygml_db_connection(cls, v: Optional[str], values: Dict[str, Any]) -> Any:
        if isinstance(v, str):
            return v
        return SyncPostgresDsn.build(
            scheme="postgresql",
            user=values.get("POSTGRES_USER_3DCITY"),
            password=values.get("POSTGRES_PASSWORD_3DCITY"),
            host=values.get("POSTGRES_HOST_3DCITY"),
            path=f"/{values.get('POSTGRES_DB_3DCITY') or ''}",
            port=values.get("POSTGRES_PORT_3DCITY")
        )
    
    # AWS Client 
    AWS_BUCKET_NAME: str = None
    AWS_ACCESS_KEY_ID: str = None
    AWS_SECRET_ACCESS_KEY: str = None
    AWS_DEFAULT_REGION: str = None
    S3_CLIENT: Optional[Any] = None
    @validator("S3_CLIENT", pre=True)
    def assemble_s3_client(cls, v: Optional[str], values: Dict[str, Any]) -> Any:
        if isinstance(v, str):
            return v
        return boto3.client(
            's3',
            aws_access_key_id=values.get("AWS_ACCESS_KEY_ID"),
            aws_secret_access_key=values.get("AWS_SECRET_ACCESS_KEY"),
            region_name=values.get("AWS_DEFAULT_REGION")
        )
    OPENROUTESERVICE_API_KEY: str = None
    GEOAPIFY_API_KEY: str = None
    GOOGLE_API_KEY: str = None
    GITHUB_ACCESS_TOKEN: str = None
    
settings = Settings()