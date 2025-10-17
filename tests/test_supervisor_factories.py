from __future__ import annotations

import asyncio

import pytest

from core.supervisor import FactoryValidationError, Supervisor


@pytest.mark.asyncio
async def test_supervisor_rejects_coroutine_when_validation_enabled() -> None:
    supervisor = Supervisor(validate_factories=True)

    async def sample_task() -> None:
        await asyncio.sleep(0)

    coroutine = sample_task()
    with pytest.raises(FactoryValidationError):
        supervisor.supervised_task(coroutine, name="bad")
    coroutine.close()

    await supervisor.shutdown()


@pytest.mark.asyncio
async def test_supervisor_rejects_factory_with_required_arguments() -> None:
    supervisor = Supervisor(validate_factories=True)

    async def needs_argument(value: str) -> None:
        await asyncio.sleep(0)

    with pytest.raises(FactoryValidationError):
        supervisor.supervised_task(needs_argument, name="missing-arg")

    supervisor.supervised_task(lambda: needs_argument("ok"), name="valid")
    await asyncio.sleep(0)

    await supervisor.shutdown()


@pytest.mark.asyncio
async def test_enable_factory_validation_allows_custom_validator() -> None:
    supervisor = Supervisor()
    seen: list[object] = []

    def validator(factory) -> None:
        seen.append(factory)
        if getattr(factory, "__name__", "").startswith("bad"):
            raise FactoryValidationError("invalid")

    supervisor.enable_factory_validation(validator=validator)

    async def good_factory() -> None:
        await asyncio.sleep(0)

    supervisor.supervised_task(good_factory, name="good")
    await asyncio.sleep(0)
    assert seen and seen[0] is good_factory

    async def bad_factory() -> None:
        await asyncio.sleep(0)

    with pytest.raises(FactoryValidationError):
        supervisor.supervised_task(bad_factory, name="bad-factory")

    await supervisor.shutdown()