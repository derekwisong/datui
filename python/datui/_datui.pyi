"""Type stub for the native _datui extension (built by maturin)."""

def view_from_bytes(
    data: bytes,
    *,
    debug: bool = False,
    row_numbers: bool = False,
) -> None: ...

def view_from_json(
    json_str: str,
    *,
    debug: bool = False,
    row_numbers: bool = False,
) -> None: ...

def run_cli() -> None: ...
