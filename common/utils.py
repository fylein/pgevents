from typing import Union, List, Dict, Any
import click


class DeserializerUtils:
    @staticmethod
    def convert_bytes_to_int(in_bytes: bytes) -> int:
        return int.from_bytes(in_bytes, byteorder='big', signed=True)

    @staticmethod
    def convert_bytes_to_utf8(in_bytes: Union[bytes, bytearray]) -> str:
        return in_bytes.decode('utf-8')


def validate_db_configs(db_configs: List[Dict[str, Any]]) -> None:
    """Validate database configurations for MultiDBEventProducer."""
    required_fields = {
        'pg_host': str,
        'pg_port': int,
        'pg_database': str,
        'pg_user': str,
        'pg_password': str,
        'pg_tables': str,
        'pg_replication_slot': str
    }

    if not isinstance(db_configs, list):
        raise click.BadParameter("db_configs must be a list of database configurations")

    if not db_configs:
        raise click.BadParameter("db_configs cannot be empty")

    for idx, config in enumerate(db_configs):
        # Check required fields
        for field, field_type in required_fields.items():
            if field not in config:
                raise click.BadParameter(f"Missing required field '{field}' in configuration at index {idx}")

            if not isinstance(config[field], field_type):
                raise click.BadParameter(
                    f"Field '{field}' in configuration at index {idx} "
                    f"must be of type {field_type.__name__}"
                )

        # Validate pg_port range
        if not (1024 <= config['pg_port'] <= 65535):
            raise click.BadParameter(
                f"Invalid port number {config['pg_port']} in configuration at index {idx}. "
                "Port must be between 1024 and 65535"
            )

        # Validate pg_tables format
        tables = config['pg_tables'].split(',')
        for table in tables:
            table = table.strip()
            if not table or '.' not in table:
                raise click.BadParameter(
                    f"Invalid table format '{table}' in configuration at index {idx}. "
                    "Format should be 'schema.table'"
                )
