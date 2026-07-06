#!/usr/bin/env python3

import sys
import json
import base64
from Crypto.Cipher import AES
from Crypto.Util.Padding import unpad

# --- Config: key/IV pulled from the client bundle ---
KEY = b"Ch@tS3cr3tK3y!16"   # 16 bytes -> AES-128
IV = b"Ch@tIV#16Bytes!!"   # 16 bytes


def decrypt_content_details(base64_ciphertext):
    """
    Decrypts a base64-encoded AES-128-CBC ciphertext.
    Returns the parsed JSON object.
    Raises an exception on failure.
    """
    ciphertext = base64.b64decode(base64_ciphertext)

    cipher = AES.new(KEY, AES.MODE_CBC, IV)
    decrypted = cipher.decrypt(ciphertext)

    plaintext = unpad(decrypted, AES.block_size).decode("utf-8")

    return json.loads(plaintext)


def main():
    if len(sys.argv) != 2:
        print(f"Usage: {sys.argv[0]} \"<base64_ciphertext>\"")
        sys.exit(1)

    ciphertext = sys.argv[1]

    try:
        result = decrypt_content_details(ciphertext)

        print("✅ Decryption succeeded:\n")
        print(json.dumps(result, indent=2, ensure_ascii=False))

    except Exception as err:
        print(f"❌ Decryption failed: {err}\n")
        print("Possible causes:")
        print("  - The string isn't valid base64 or was truncated.")
        print("  - The key/IV don't match this payload.")
        print("  - The payload wasn't encrypted with AES-128-CBC.")
        print("  - The decrypted data isn't valid JSON.")
        sys.exit(1)


if __name__ == "__main__":
    main()
