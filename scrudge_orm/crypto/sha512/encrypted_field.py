import hashlib
from typing import Callable, Generic, Self, TypeVar

T = TypeVar("T", str, bytes)


class SHA512EncryptedField(Generic[T]):
    start_sequence: T  # start_sequence is a historic way

    def __new__(cls, value: T) -> "Self":
        # value will be decrypted on instance creation process if it's encrypted
        return super().__new__(cls, cls.encrypt_value(value))  # type: ignore

    @classmethod
    def _encrypt_function(cls, value: T) -> T:
        raise NotImplementedError()

    @classmethod
    def get_encrypt_function(cls) -> Callable:
        return cls._encrypt_function

    @classmethod
    def encrypt_value(cls, value: T) -> T:
        if not value.startswith(cls.start_sequence):
            try:
                final_value = cls.get_encrypt_function()(value)
            except (UnicodeDecodeError, ValueError):
                final_value = value
        else:
            final_value = value

        return final_value

    def encrypt(self) -> T:
        return self.encrypt_value(self)  # type: ignore


class SHA512EncryptedString(SHA512EncryptedField, str):
    start_sequence = "sha512:"

    @classmethod
    def _encrypt_function(cls, value: str) -> str:
        return cls.start_sequence + hashlib.sha512(value.encode("utf-8")).hexdigest()


class SHA512EncryptedBytes(SHA512EncryptedField, bytes):
    start_sequence = b"sha512:"

    @classmethod
    def _encrypt_function(cls, value: bytes) -> bytes:
        return cls.start_sequence + hashlib.sha512(value).digest()
