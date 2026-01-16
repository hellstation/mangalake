from __future__ import annotations
import io
import logging
from typing import Iterable, List
import boto3
from botocore.config import Config as BotoConfig
from etl.config import settings

logger = logging.getLogger(__name__)

class MinIOClient:
    def __init__(self, endpoint: str = settings.minio_endpoint,
                 access_key: str = settings.minio_access_key,
                 secret_key: str = settings.minio_secret_key,
                 bucket: str = settings.minio_bucket):
        self.bucket = bucket
        session = boto3.session.Session()
        self._s3 = session.client(
            "s3",
            endpoint_url=endpoint,
            aws_access_key_id=access_key,
            aws_secret_access_key=secret_key,
            config=BotoConfig(s3={"addressing_style": "path"}),
        )

    def upload_bytes(self, key: str, data: bytes, content_type: str = "application/json") -> None:
        try:
            self._s3.put_object(Bucket=self.bucket, Key=key, Body=data, ContentType=content_type)
            logger.debug("Uploaded %d bytes to %s", len(data), key)
        except Exception as e:
            logger.error("Failed to upload to %s: %r", key, e)
            raise

    def list_keys(self, prefix: str) -> List[str]:
        keys: List[str] = []
        continuation = None
        try:
            while True:
                kw = {"Bucket": self.bucket, "Prefix": prefix}
                if continuation:
                    kw["ContinuationToken"] = continuation
                resp = self._s3.list_objects_v2(**kw)
                for it in resp.get("Contents", []):
                    keys.append(it["Key"])
                if resp.get("IsTruncated"):
                    continuation = resp.get("NextContinuationToken")
                else:
                    break
        except Exception as e:
            logger.error("Failed to list keys with prefix %s: %r", prefix, e)
            raise
        return keys

    def read_bytes(self, key: str) -> bytes:
        try:
            obj = self._s3.get_object(Bucket=self.bucket, Key=key)
            data = obj["Body"].read()
            logger.debug("Read %d bytes from %s", len(data), key)
            return data
        except Exception as e:
            logger.error("Failed to read from %s: %r", key, e)
            raise

    def upload_csv_bytes(self, key: str, data: bytes) -> None:
        self.upload_bytes(key, data, "text/csv")

# Global instance for backward compatibility
minio_client = MinIOClient()

# Backward compatibility functions
def upload_bytes(key: str, data: bytes, content_type: str = "application/json") -> None:
    minio_client.upload_bytes(key, data, content_type)

def list_keys(prefix: str) -> List[str]:
    return minio_client.list_keys(prefix)

def read_bytes(key: str) -> bytes:
    return minio_client.read_bytes(key)

def upload_csv_bytes(key: str, data: bytes) -> None:
    minio_client.upload_csv_bytes(key, data)
