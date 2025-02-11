import base64
import hashlib
from typing import Union

from Crypto import Random
from Crypto.Cipher import AES


class AESCipher:
    def __init__(self, key: str):
        self.key = hashlib.sha256(key.encode()).digest()

    def encrypt_raw(self, plain: Union[bytes, str]) -> bytes:
        plain_bytes = self._pad(plain)
        iv = Random.new().read(AES.block_size)
        cipher = AES.new(self.key, AES.MODE_CBC, iv)
        return base64.b64encode(iv + cipher.encrypt(plain_bytes))

    def encrypt(self, plain: str) -> str:
        return self.encrypt_raw(plain).decode("utf-8")

    def decrypt_raw(self, encrypted: Union[bytes, str]) -> bytes:
        encrypted_bytes = base64.b64decode(encrypted)
        iv = encrypted_bytes[: AES.block_size]
        cipher = AES.new(self.key, AES.MODE_CBC, iv)
        return self._unpad(cipher.decrypt(encrypted_bytes[AES.block_size :]))

    def decrypt(self, encrypted: Union[bytes, str]) -> str:
        return self.decrypt_raw(encrypted).decode("utf-8")

    @staticmethod
    def _pad(s: Union[str, bytes]) -> bytes:
        bs = AES.block_size

        if isinstance(s, str):
            s = s.encode()

        return s + (bs - len(s) % bs) * (chr(bs - len(s) % bs).encode())

    @staticmethod
    def _unpad(s: bytes) -> bytes:
        return s[: -ord(s[len(s) - 1 :])]
