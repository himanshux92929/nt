#!/usr/bin/env python3
"""
Patched decrypt_content_details with diagnostics.

Drop-in replacement for the function of the same name in bot.py.
Only this function is touched — nothing else in bot.py needs to change.
"""
import sys
import json
import base64
import logging
from Crypto.Cipher import AES
from Crypto.Util.Padding import unpad

logger = logging.getLogger(__name__)

# --- Config: key/IV pulled from the client bundle ---
KEY = b"Ch@tS3cr3tK3y!16"   # 16 bytes -> AES-128
IV = b"Ch@tIV#16Bytes!!"    # 16 bytes


class DecryptDiagnosticError(Exception):
    """Raised with extra context when decryption produces unusable output."""
    pass


def decrypt_content_details(base64_ciphertext, content_id=None):
    """
    Decrypts a base64-encoded AES-128-CBC ciphertext.
    Returns the parsed JSON object.

    Raises DecryptDiagnosticError with detailed context on failure,
    instead of letting json.loads raise an opaque
    "Expecting value: line 1 column 1 (char 0)" error.
    """
    label = f"content_id={content_id}" if content_id is not None else "content_id=<unknown>"

    # --- Step 1: base64 decode ---
    if not base64_ciphertext:
        raise DecryptDiagnosticError(
            f"[{label}] base64_ciphertext is empty or None before decoding."
        )

    try:
        ciphertext = base64.b64decode(base64_ciphertext)
    except Exception as e:
        raise DecryptDiagnosticError(
            f"[{label}] base64 decode failed: {e}. "
            f"Input length={len(base64_ciphertext)} chars."
        ) from e

    # --- Step 2: sanity-check ciphertext length ---
    if len(ciphertext) == 0:
        raise DecryptDiagnosticError(
            f"[{label}] Decoded ciphertext is empty (0 bytes). "
            f"The stored/base64 value for this record is likely missing or truncated."
        )

    if len(ciphertext) % AES.block_size != 0:
        raise DecryptDiagnosticError(
            f"[{label}] Ciphertext length ({len(ciphertext)} bytes) is not a "
            f"multiple of the AES block size ({AES.block_size}). "
            f"This payload is truncated/corrupted and was not encrypted with "
            f"AES-CBC using this block size."
        )

    # --- Step 3: AES-CBC decrypt ---
    try:
        cipher = AES.new(KEY, AES.MODE_CBC, IV)
        decrypted = cipher.decrypt(ciphertext)
    except Exception as e:
        raise DecryptDiagnosticError(
            f"[{label}] AES-CBC decrypt call failed: {e}"
        ) from e

    # --- Step 4: unpad, with visibility into what's being stripped ---
    try:
        plaintext_bytes = unpad(decrypted, AES.block_size)
    except Exception as e:
        raise DecryptDiagnosticError(
            f"[{label}] Padding removal failed: {e}. "
            f"Last block (hex)={decrypted[-AES.block_size:].hex()}. "
            f"This strongly suggests the KEY/IV do not match this payload."
        ) from e

    if len(plaintext_bytes) == 0:
        raise DecryptDiagnosticError(
            f"[{label}] Decryption + unpad succeeded but produced an EMPTY "
            f"plaintext (0 bytes after stripping padding). "
            f"Decrypted length before unpad={len(decrypted)} bytes. "
            f"This is the signature of a key/IV mismatch: the cipher runs "
            f"without error, but the output is garbage that happens to look "
            f"like a full block of valid padding."
        )

    # --- Step 5: decode + JSON parse ---
    try:
        plaintext = plaintext_bytes.decode("utf-8")
    except UnicodeDecodeError as e:
        raise DecryptDiagnosticError(
            f"[{label}] UTF-8 decode failed after unpad: {e}. "
            f"Plaintext byte length={len(plaintext_bytes)}. "
            f"First 32 bytes (hex)={plaintext_bytes[:32].hex()}. "
            f"Likely a key/IV mismatch producing non-UTF-8 garbage."
        ) from e

    try:
        return json.loads(plaintext)
    except json.JSONDecodeError as e:
        raise DecryptDiagnosticError(
            f"[{label}] JSON parse failed: {e}. "
            f"Plaintext length={len(plaintext)} chars. "
            f"Plaintext preview (first 80 chars)={plaintext[:80]!r}."
        ) from e


def main():
    if len(sys.argv) != 2:
        print(f"Usage: {sys.argv[0]} \"<base64_ciphertext>\"")
        sys.exit(1)

    ciphertext = sys.argv[1]
    try:
        result = decrypt_content_details(ciphertext)
        print("✅ Decryption succeeded:\n")
        print(json.dumps(result, indent=2, ensure_ascii=False))
    except DecryptDiagnosticError as err:
        print(f"❌ Decryption failed with diagnostics:\n{err}\n")
        sys.exit(1)
    except Exception as err:
        print(f"❌ Unexpected error: {err}\n")
        sys.exit(1)


if __name__ == "__main__":
    main()
