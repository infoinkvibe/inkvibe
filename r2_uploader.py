from __future__ import annotations

import mimetypes
import os
import pathlib
from dataclasses import dataclass
from typing import Dict, Optional

try:
    import boto3
except Exception:  # pragma: no cover - optional dependency handled at runtime
    boto3 = None


@dataclass
class R2Config:
    account_id: str
    access_key_id: str
    secret_access_key: str
    bucket: str
    public_base_url: str

    @property
    def endpoint_url(self) -> str:
        return f"https://{self.account_id}.r2.cloudflarestorage.com"


def load_r2_config_from_env() -> Optional[R2Config]:
    values: Dict[str, str] = {
        "account_id": os.getenv("R2_ACCOUNT_ID", "").strip(),
        "access_key_id": os.getenv("R2_ACCESS_KEY_ID", "").strip(),
        "secret_access_key": os.getenv("R2_SECRET_ACCESS_KEY", "").strip(),
        "bucket": os.getenv("R2_BUCKET", "").strip(),
        "public_base_url": os.getenv("R2_PUBLIC_BASE_URL", "").strip().rstrip("/"),
    }
    if all(values.values()):
        return R2Config(**values)
    return None


def build_r2_public_url(public_base_url: str, object_key: str) -> str:
    return f"{public_base_url.rstrip('/')}/{object_key.lstrip('/')}"


def upload_file_to_r2(file_path: pathlib.Path, object_key: str, config: R2Config) -> str:
    if boto3 is None:
        raise RuntimeError("boto3 is required for R2 uploads. Install dependencies from requirements.txt")

    s3 = boto3.client(
        "s3",
        endpoint_url=config.endpoint_url,
        aws_access_key_id=config.access_key_id,
        aws_secret_access_key=config.secret_access_key,
        region_name="auto",
    )
    content_type = mimetypes.guess_type(file_path.name)[0] or "application/octet-stream"
    s3.upload_file(str(file_path), config.bucket, object_key, ExtraArgs={"ContentType": content_type})
    return build_r2_public_url(config.public_base_url, object_key)
