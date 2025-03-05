from typing import Union, List, Dict, Any
from datetime import datetime, timezone

class DeserializerUtils:
    @staticmethod
    def convert_bytes_to_int(in_bytes: bytes) -> int:
        return int.from_bytes(in_bytes, byteorder='big', signed=True)

    @staticmethod
    def convert_bytes_to_utf8(in_bytes: Union[bytes, bytearray]) -> str:
        return in_bytes.decode('utf-8')

def get_utc_now() -> datetime:
    return datetime.now(timezone.utc)
