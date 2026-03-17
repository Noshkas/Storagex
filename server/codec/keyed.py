from __future__ import annotations

import hashlib
import re
from typing import BinaryIO, Iterator

import numpy as np

from .constants import KEY_CHUNK_BYTES

KEY_LENGTH = 24
KEY_PATTERN = re.compile(r"^\d{24}$")
MASK64 = (1 << 64) - 1
LEGACY_KEY_MODE = "custom24-scramble"
STREAM_KEY_MODE = "custom24-shake256-chunk-xor"


def validate_numeric_key(key: str) -> str:
    if not KEY_PATTERN.fullmatch(key):
        raise ValueError("Key must be exactly 24 digits.")
    return key


def scramble_payload(payload: bytes, key: str) -> bytes:
    normalized_key = validate_numeric_key(key)
    if not payload:
        return b""

    digits = _digits(normalized_key)
    keystream = _keystream(_derive_seed(digits), len(payload))

    xored = bytes(byte ^ keystream[index] for index, byte in enumerate(payload))
    rotated = bytes(
        _rotate_left_byte(byte, ((digits[index % KEY_LENGTH] + index) % 7) + 1)
        for index, byte in enumerate(xored)
    )

    shift = ((sum(digits) * 97) + len(rotated)) % len(rotated)
    if shift == 0:
        return rotated
    return rotated[-shift:] + rotated[:-shift]


def unscramble_payload(payload: bytes, key: str) -> bytes:
    normalized_key = validate_numeric_key(key)
    if not payload:
        return b""

    digits = _digits(normalized_key)
    shift = ((sum(digits) * 97) + len(payload)) % len(payload)
    if shift == 0:
        unshifted = payload
    else:
        unshifted = payload[shift:] + payload[:shift]

    unrotated = bytes(
        _rotate_right_byte(byte, ((digits[index % KEY_LENGTH] + index) % 7) + 1)
        for index, byte in enumerate(unshifted)
    )
    keystream = _keystream(_derive_seed(digits), len(unrotated))
    return bytes(byte ^ keystream[index] for index, byte in enumerate(unrotated))


def stream_payload_transform(source: BinaryIO, *, key: str, chunk_size: int = KEY_CHUNK_BYTES) -> Iterator[bytes]:
    normalized_key = validate_numeric_key(key)
    seed = _stream_seed(normalized_key)
    chunk_index = 0
    while True:
        chunk = source.read(chunk_size)
        if not chunk:
            break
        yield transform_payload_chunk(chunk, seed=seed, chunk_index=chunk_index)
        chunk_index += 1


def transform_payload_chunk(chunk: bytes, *, seed: bytes, chunk_index: int) -> bytes:
    if not chunk:
        return b""
    keystream = hashlib.shake_256(seed + chunk_index.to_bytes(8, "little")).digest(len(chunk))
    transformed = np.bitwise_xor(
        np.frombuffer(chunk, dtype=np.uint8),
        np.frombuffer(keystream, dtype=np.uint8),
    )
    return transformed.tobytes()


def stream_payload_transform_to_file(
    source: BinaryIO,
    target: BinaryIO,
    *,
    key: str,
    chunk_size: int = KEY_CHUNK_BYTES,
) -> int:
    total_bytes = 0
    for chunk in stream_payload_transform(source, key=key, chunk_size=chunk_size):
        target.write(chunk)
        total_bytes += len(chunk)
    return total_bytes


def _stream_seed(key: str) -> bytes:
    normalized_key = validate_numeric_key(key)
    return hashlib.sha256(normalized_key.encode("ascii")).digest()


def _digits(key: str) -> list[int]:
    return [int(char) for char in key]


def _derive_seed(digits: list[int]) -> int:
    seed = 0x9E3779B97F4A7C15
    for digit in digits:
        seed ^= ((digit + 1) * 0xA24BAED4963EE407) & MASK64
        seed = _rotate_left_64(seed, 7)
        seed = (seed * 0x9FB21C651E98DF25 + 0xD1B54A32D192ED03) & MASK64
    if seed == 0:
        return 0xA5A5A5A5A5A5A5A5
    return seed


def _keystream(seed: int, length: int) -> bytes:
    stream = bytearray(length)
    state = seed & MASK64
    for index in range(length):
        state ^= state >> 12
        state &= MASK64
        state ^= (state << 25) & MASK64
        state &= MASK64
        state ^= state >> 27
        state &= MASK64
        state = (state * 0x2545F4914F6CDD1D) & MASK64
        stream[index] = (state >> 56) & 0xFF
    return bytes(stream)


def _rotate_left_64(value: int, count: int) -> int:
    return ((value << count) | (value >> (64 - count))) & MASK64


def _rotate_left_byte(value: int, count: int) -> int:
    count %= 8
    return ((value << count) | (value >> (8 - count))) & 0xFF


def _rotate_right_byte(value: int, count: int) -> int:
    count %= 8
    return ((value >> count) | (value << (8 - count))) & 0xFF
