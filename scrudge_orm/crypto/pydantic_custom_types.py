from typing import Annotated

from pydantic import BeforeValidator, WithJsonSchema

from scrudge_orm.crypto.sha512.encrypted_field import SHA512EncryptedString

SHA512EncryptedStringAnnotation = Annotated[
    str, BeforeValidator(lambda v: SHA512EncryptedString(v)), WithJsonSchema({"type": "string"}, mode="serialization")
]
