import pickle

import pytest

from scrudge_orm.crypto.sha512.encrypted_field import SHA512EncryptedString
from tests.test_scrudge_orm.models import UnitTestSHA512CryptoModel


class TestSHA512CryptoField:
    field_str_value = "unit_test_string"
    schema = UnitTestSHA512CryptoModel

    def test_encryption_flow(self) -> None:
        obj = self.schema(field_str=self.field_str_value)

        # automatically encrypted
        assert isinstance(obj.field_str, str)
        assert obj.field_str == SHA512EncryptedString.encrypt_value(self.field_str_value)

    def test_pickle(self) -> None:
        obj = self.schema(field_str=self.field_str_value)
        pickled_obj = pickle.dumps(obj)

        assert obj == pickle.loads(pickled_obj)

    @pytest.mark.asyncio
    async def test_pg(self) -> None:
        obj = self.schema(field_str=self.field_str_value)
        inserted_obj: UnitTestSHA512CryptoModel = await self.schema.objects.create(**obj.model_dump())

        assert inserted_obj is not None
        assert obj == inserted_obj
