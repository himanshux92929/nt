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


def decrypt_with_logging(base64_ciphertext, content_id=None, course=None, logger_=None):
    """
    Wrapper matching the existing call-site logging pattern
    (e.g. "decrypt.py invocation error content_id=... (course=...)").

    Instead of logging a generic "No detail for content_id=X, will retry",
    this logs the specific DecryptDiagnosticError reason, so each failure
    in the logs says WHY (empty ciphertext, bad length, key/IV mismatch,
    empty plaintext, bad utf-8, or bad JSON) rather than just "no detail".

    Returns the parsed JSON dict on success, or None on failure (after logging).
    Use this in place of a bare try/except around decrypt_content_details
    at call sites that currently log "No detail for content_id=... will retry".
    """
    log = logger_ or logger
    try:
        return decrypt_content_details(base64_ciphertext, content_id=content_id)
    except DecryptDiagnosticError as err:
        course_str = f" (course={course})" if course is not None else ""
        log.warning(
            "decrypt.py invocation error content_id=%s%s: %s",
            content_id, course_str, err
        )
        return None
    except Exception as err:
        course_str = f" (course={course})" if course is not None else ""
        log.warning(
            "decrypt.py invocation error content_id=%s%s: unexpected error: %s",
            content_id, course_str, err
        )
        return None


def main():
    # NOTE: this is invoked as a subprocess by bot.py, which does
    # json.loads(stdout) directly. stdout must therefore contain ONLY
    # the raw JSON on success — no banners, no pretty-printing, no
    # emoji lines. Anything human-readable (diagnostics, errors) goes
    # to stderr instead, so bot.py's stderr-based logging still works.
    if len(sys.argv) != 2:
        print("Usage: decrypt.py \"<base64_ciphertext>\"", file=sys.stderr)
        sys.exit(2)  # exit code 2 = bad invocation (argc)

    ciphertext = sys.argv[1]
    try:
        result = decrypt_content_details(ciphertext)
        # stdout: machine-readable JSON only, compact (no indent needed
        # for a subprocess pipe, but harmless either way)
        print(json.dumps(result, ensure_ascii=False))
        sys.exit(0)
    except DecryptDiagnosticError as err:
        print(str(err), file=sys.stderr)
        sys.exit(1)  # exit code 1 = diagnosed decrypt/parse failure
    except Exception as err:
        print(f"Unexpected error: {err}", file=sys.stderr)
        sys.exit(3)  # exit code 3 = unexpected/unhandled failure


if __name__ == "__main__":
    main()
