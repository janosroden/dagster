from typing import Generator, Tuple

import pytest
from dagster import (
    AssetCheckResult,
    AssetCheckSpec,
    AssetExecutionContext,
    AssetKey,
    AssetOut,
    AssetSpec,
    IOManager,
    MaterializeResult,
    StaticPartitionsDefinition,
    asset,
    build_op_context,
    instance_for_test,
    materialize,
    multi_asset,
)
from dagster._core.errors import DagsterInvariantViolationError, DagsterStepOutputNotFoundError
from dagster._core.storage.asset_check_execution_record import AssetCheckExecutionRecordStatus


def _exec_asset(asset_def, selection=None, partition_key=None):
    result = materialize([asset_def], selection=selection, partition_key=partition_key)
    assert result.success
    return result.asset_materializations_for_node(asset_def.node_def.name)


def test_materialize_result_asset():
    @asset
    def ret_untyped(context: AssetExecutionContext):
        return MaterializeResult(
            metadata={"one": 1},
        )

    mats = _exec_asset(ret_untyped)
    assert len(mats) == 1, mats
    assert "one" in mats[0].metadata
    assert mats[0].tags

    # key mismatch
    @asset
    def ret_mismatch(context: AssetExecutionContext):
        return MaterializeResult(
            asset_key="random",
            metadata={"one": 1},
        )

    with pytest.raises(
        DagsterInvariantViolationError,
        match="Asset key random not found in AssetsDefinition",
    ):
        materialize([ret_mismatch])

    @asset
    def ret_two():
        return MaterializeResult(metadata={"one": 1}), MaterializeResult(metadata={"two": 2})

    materialize([ret_two])


def test_return_materialization_with_asset_checks():
    with instance_for_test() as instance:

        @asset(check_specs=[AssetCheckSpec(name="foo_check", asset=AssetKey("ret_checks"))])
        def ret_checks(context: AssetExecutionContext):
            return MaterializeResult(
                check_results=[
                    AssetCheckResult(check_name="foo_check", metadata={"one": 1}, passed=True)
                ]
            )

        materialize([ret_checks], instance=instance)
        asset_check_executions = instance.event_log_storage.get_asset_check_executions(
            asset_key=ret_checks.key,
            check_name="foo_check",
            limit=1,
        )
        assert len(asset_check_executions) == 1
        assert asset_check_executions[0].status == AssetCheckExecutionRecordStatus.SUCCEEDED


def test_multi_asset():
    @multi_asset(outs={"one": AssetOut(), "two": AssetOut()})
    def outs_multi_asset():
        return MaterializeResult(asset_key="one", metadata={"foo": "bar"}), MaterializeResult(
            asset_key="two", metadata={"baz": "qux"}
        )

    materialize([outs_multi_asset])

    @multi_asset(specs=[AssetSpec(["prefix", "one"]), AssetSpec(["prefix", "two"])])
    def specs_multi_asset():
        return MaterializeResult(
            asset_key=["prefix", "one"], metadata={"foo": "bar"}
        ), MaterializeResult(asset_key=["prefix", "two"], metadata={"baz": "qux"})

    materialize([specs_multi_asset])


def test_return_materialization_multi_asset():
    #
    # yield successful
    #
    @multi_asset(outs={"one": AssetOut(), "two": AssetOut()})
    def multi():
        yield MaterializeResult(
            asset_key="one",
            metadata={"one": 1},
        )
        yield MaterializeResult(
            asset_key="two",
            metadata={"two": 2},
        )

    mats = _exec_asset(multi)

    assert len(mats) == 2, mats
    assert "one" in mats[0].metadata
    assert mats[0].tags
    assert "two" in mats[1].metadata
    assert mats[1].tags

    #
    # missing a non optional out
    #
    @multi_asset(outs={"one": AssetOut(), "two": AssetOut()})
    def missing():
        yield MaterializeResult(
            asset_key="one",
            metadata={"one": 1},
        )

    # currently a less than ideal error
    with pytest.raises(
        DagsterStepOutputNotFoundError,
        match=(
            'Core compute for op "missing" did not return an output for non-optional output "two"'
        ),
    ):
        _exec_asset(missing)

    #
    # missing asset_key
    #
    @multi_asset(outs={"one": AssetOut(), "two": AssetOut()})
    def no_key():
        yield MaterializeResult(
            metadata={"one": 1},
        )
        yield MaterializeResult(
            metadata={"two": 2},
        )

    with pytest.raises(
        DagsterInvariantViolationError,
        match=(
            "MaterializeResult did not include asset_key and it can not be inferred. Specify which"
            " asset_key, options are:"
        ),
    ):
        _exec_asset(no_key)

    #
    # return tuple success
    #
    @multi_asset(outs={"one": AssetOut(), "two": AssetOut()})
    def ret_multi():
        return (
            MaterializeResult(
                asset_key="one",
                metadata={"one": 1},
            ),
            MaterializeResult(
                asset_key="two",
                metadata={"two": 2},
            ),
        )

    mats = _exec_asset(ret_multi)

    assert len(mats) == 2, mats
    assert "one" in mats[0].metadata
    assert mats[0].tags
    assert "two" in mats[1].metadata
    assert mats[1].tags

    #
    # return list error
    #
    @multi_asset(outs={"one": AssetOut(), "two": AssetOut()})
    def ret_list():
        return [
            MaterializeResult(
                asset_key="one",
                metadata={"one": 1},
            ),
            MaterializeResult(
                asset_key="two",
                metadata={"two": 2},
            ),
        ]

    # not the best
    with pytest.raises(
        DagsterInvariantViolationError,
        match=(
            "When using multiple outputs, either yield each output, or return a tuple containing a"
            " value for each output."
        ),
    ):
        _exec_asset(ret_list)


def test_materialize_result_output_typing():
    # Test that the return annotation MaterializeResult is interpreted as a Nothing type, since we
    # coerce returned MaterializeResults to Output(None)

    class TestingIOManager(IOManager):
        def handle_output(self, context, obj):
            assert context.dagster_type.is_nothing
            return None

        def load_input(self, context):
            return 1

    @asset
    def asset_with_type_annotation() -> MaterializeResult:
        return MaterializeResult(metadata={"foo": "bar"})

    assert materialize(
        [asset_with_type_annotation], resources={"io_manager": TestingIOManager()}
    ).success

    @multi_asset(outs={"one": AssetOut(), "two": AssetOut()})
    def multi_asset_with_outs_and_type_annotation() -> Tuple[MaterializeResult, MaterializeResult]:
        return MaterializeResult(asset_key="one"), MaterializeResult(asset_key="two")

    assert materialize(
        [multi_asset_with_outs_and_type_annotation], resources={"io_manager": TestingIOManager()}
    ).success

    @multi_asset(specs=[AssetSpec("one"), AssetSpec("two")])
    def multi_asset_with_specs_and_type_annotation() -> Tuple[MaterializeResult, MaterializeResult]:
        return MaterializeResult(asset_key="one"), MaterializeResult(asset_key="two")

    assert materialize(
        [multi_asset_with_specs_and_type_annotation], resources={"io_manager": TestingIOManager()}
    ).success

    @multi_asset(specs=[AssetSpec("one"), AssetSpec("two")])
    def multi_asset_with_specs_and_no_type_annotation():
        return MaterializeResult(asset_key="one"), MaterializeResult(asset_key="two")

    assert materialize(
        [multi_asset_with_specs_and_no_type_annotation],
        resources={"io_manager": TestingIOManager()},
    ).success


@pytest.mark.skip(
    "Generator return types are interpreted as Any. See"
    " https://github.com/dagster-io/dagster/pull/16906"
)
def test_generator_return_type_annotation():
    class TestingIOManager(IOManager):
        def handle_output(self, context, obj):
            assert context.dagster_type.is_nothing
            return None

        def load_input(self, context):
            return 1

    @asset
    def generator_asset() -> Generator[MaterializeResult, None, None]:
        yield MaterializeResult(metadata={"foo": "bar"})

    materialize([generator_asset], resources={"io_manager": TestingIOManager()})


@pytest.mark.skip(
    "Direct invocation causes errors, unskip these tests once direct invocation path is fixed."
    " https://github.com/dagster-io/dagster/issues/16921"
)
def test_direct_invocation_materialize_result():
    @asset
    def my_asset() -> MaterializeResult:
        return MaterializeResult(metadata={"foo": "bar"})

    res = my_asset()
    assert res.metadata["foo"] == "bar"

    @multi_asset(specs=[AssetSpec("one"), AssetSpec("two")])
    def specs_multi_asset():
        return MaterializeResult(asset_key="one", metadata={"foo": "bar"}), MaterializeResult(
            asset_key="two", metadata={"baz": "qux"}
        )

    res = specs_multi_asset()
    assert res[0].metadata["foo"] == "bar"
    assert res[1].metadata["baz"] == "qux"

    @multi_asset(outs={"one": AssetOut(), "two": AssetOut()})
    def outs_multi_asset():
        return MaterializeResult(asset_key="one", metadata={"foo": "bar"}), MaterializeResult(
            asset_key="two", metadata={"baz": "qux"}
        )

    res = outs_multi_asset()
    assert res[0].metadata["foo"] == "bar"
    assert res[1].metadata["baz"] == "qux"


@pytest.mark.skip(
    "Direct invocation causes errors, unskip these tests once direct invocation path is fixed."
    " https://github.com/dagster-io/dagster/issues/16921"
)
def test_direct_invocation_for_generators():
    @asset
    def generator_asset() -> Generator[MaterializeResult, None, None]:
        yield MaterializeResult(metadata={"foo": "bar"})

    res = list(generator_asset())
    assert res[0].metadata["foo"] == "bar"

    @multi_asset(specs=[AssetSpec("one"), AssetSpec("two")])
    def generator_specs_multi_asset():
        yield MaterializeResult(asset_key="one", metadata={"foo": "bar"})
        yield MaterializeResult(asset_key="two", metadata={"baz": "qux"})

    res = list(generator_specs_multi_asset())
    assert res[0].metadata["foo"] == "bar"
    assert res[1].metadata["baz"] == "qux"

    @multi_asset(outs={"one": AssetOut(), "two": AssetOut()})
    def generator_outs_multi_asset():
        yield MaterializeResult(asset_key="one", metadata={"foo": "bar"})
        yield MaterializeResult(asset_key="two", metadata={"baz": "qux"})

    res = list(generator_outs_multi_asset())
    assert res[0].metadata["foo"] == "bar"
    assert res[1].metadata["baz"] == "qux"

    # need to test async generator case and coroutine see op_invocation.py:_type_check_output_wrapper for all cases


def test_materialize_result_with_partitions():
    @asset(partitions_def=StaticPartitionsDefinition(["red", "blue", "yellow"]))
    def partitioned_asset(context: AssetExecutionContext) -> MaterializeResult:
        return MaterializeResult(metadata={"key": context.partition_key})

    mats = _exec_asset(partitioned_asset, partition_key="red")
    assert len(mats) == 1, mats
    assert mats[0].metadata["key"].text == "red"


@pytest.mark.skip(
    "Direct invocation causes errors, unskip these tests once direct invocation path is fixed."
    " https://github.com/dagster-io/dagster/issues/16921"
)
def test_materialize_result_with_partitions_direct_invocation():
    @asset(partitions_def=StaticPartitionsDefinition(["red", "blue", "yellow"]))
    def partitioned_asset(context: AssetExecutionContext) -> MaterializeResult:
        return MaterializeResult(metadata={"key": context.partition_key})

    context = build_op_context(partition_key="red")

    res = partitioned_asset(context)
    assert res.metadata["key"] == "red"
