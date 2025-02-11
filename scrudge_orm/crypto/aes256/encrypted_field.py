import sys
from typing import Callable, Self, Type, Union

from scrudge_orm.crypto.aes256.cipher import AESCipher


class AES256EncryptedField:
    cipher: AESCipher
    start_sequence: Union[str, bytes]

    def __new__(cls, value: Union[str, bytes]) -> "Self":
        # value will be decrypted on instance creation process if it's encrypted
        return super().__new__(cls, cls.decrypt_value(value))  # type: ignore

    @classmethod
    def get_encrypt_function(cls) -> Callable:
        raise NotImplementedError()

    @classmethod
    def get_decrypt_function(cls) -> Callable:
        raise NotImplementedError()

    @classmethod
    def decrypt_value(cls, value: Union[bytes, str, Self]) -> Self:
        if value.startswith(cls.start_sequence):  # type: ignore
            try:
                final_value = cls.get_decrypt_function()(value[7:])  # type: ignore
            except (UnicodeDecodeError, ValueError):
                final_value = value
        else:
            final_value = value

        return final_value

    def decrypt(self) -> Self:
        return self.decrypt_value(self)

    def encrypt(self) -> Self:
        return self.start_sequence + self.get_encrypt_function()(self)

    @classmethod
    def create_cls(cls, cls_identifier: str, aes_key: str) -> Type[Self]:
        """
        Dynamically creates new class with provided encryption key
        :param cls_identifier: should be unique for python module
        :param aes_key: AES encryption key
        :return: class type object
        """
        # need to register class in module due to pickle issues
        module = sys.modules[__name__]
        class_name = f"{cls.__name__}{cls_identifier}"
        klass = type(class_name, (cls,), {"cipher": AESCipher(aes_key)})
        setattr(module, class_name, klass)

        return klass


class AES256EncryptedString(AES256EncryptedField, str):
    start_sequence = "aes256:"

    @classmethod
    def get_encrypt_function(cls) -> Callable:
        return cls.cipher.encrypt

    @classmethod
    def get_decrypt_function(cls) -> Callable:
        return cls.cipher.decrypt


class AES256EncryptedBytes(AES256EncryptedField, bytes):
    start_sequence = b"aes256:"

    @classmethod
    def get_encrypt_function(cls) -> Callable:
        return cls.cipher.encrypt_raw

    @classmethod
    def get_decrypt_function(cls) -> Callable:
        return cls.cipher.decrypt_raw
