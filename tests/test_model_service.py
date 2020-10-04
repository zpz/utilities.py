import asyncio
import pytest

from coyote.model_service import Modelet, ModelService


class Scale(Modelet):
    def predict(self, x):
        return x * 2


class Shift(Modelet):
    def predict(self, x):
        return x + 3


@pytest.mark.asyncio
async def test_service():
    service = ModelService(cpus=[0])
    service.add_modelet(Scale, cpus=[1,2])
    service.add_modelet(Shift, cpus=[3])
    with service:
        z = await service.a_predict(3)
        assert z == 3 * 2 + 3

        x = list(range(10))
        tasks = [service.a_predict(v) for v in x]
        y = await asyncio.gather(*tasks)
        assert y == [v * 2 + 3 for v in x]
