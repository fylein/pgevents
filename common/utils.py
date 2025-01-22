from typing import Union, List, Dict, Any
import click


class DeserializerUtils:
    @staticmethod
    def convert_bytes_to_int(in_bytes: bytes) -> int:
        return int.from_bytes(in_bytes, byteorder='big', signed=True)

    @staticmethod
    def convert_bytes_to_utf8(in_bytes: Union[bytes, bytearray]) -> str:
        return in_bytes.decode('utf-8')
