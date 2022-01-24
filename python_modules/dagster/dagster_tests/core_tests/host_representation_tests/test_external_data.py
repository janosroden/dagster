import pytest
from dagster import AssetKey, DagsterInvariantViolationError, Out
from dagster.check import CheckError
from dagster.core.asset_defs import ForeignAsset, asset, build_assets_job, multi_asset
from dagster.core.host_representation.external_data import (
    ExternalAssetDependedBy,
    ExternalAssetDependency,
    ExternalAssetNode,
    ExternalSensorData,
    ExternalTargetData,
    external_asset_graph_from_defs,
)
from dagster.serdes import deserialize_json_to_dagster_namedtuple


def test_single_asset_job():
    @asset
    def asset1():
        return 1

    assets_job = build_assets_job("assets_job", [asset1])
    external_asset_nodes = external_asset_graph_from_defs([assets_job], foreign_assets_by_key={})

    assert external_asset_nodes == [
        ExternalAssetNode(
            asset_key=AssetKey("asset1"),
            dependencies=[],
            depended_by=[],
            op_name="asset1",
            op_description=None,
            job_names=["assets_job"],
            output_name="result",
            output_description=None,
        )
    ]


def test_two_asset_job():
    @asset
    def asset1():
        return 1

    @asset
    def asset2(asset1):
        assert asset1 == 1

    assets_job = build_assets_job("assets_job", [asset1, asset2])
    external_asset_nodes = external_asset_graph_from_defs([assets_job], foreign_assets_by_key={})

    assert external_asset_nodes == [
        ExternalAssetNode(
            asset_key=AssetKey("asset1"),
            dependencies=[],
            depended_by=[
                ExternalAssetDependedBy(
                    downstream_asset_key=AssetKey("asset2"), input_name="asset1"
                )
            ],
            op_name="asset1",
            op_description=None,
            job_names=["assets_job"],
            output_name="result",
            output_description=None,
        ),
        ExternalAssetNode(
            asset_key=AssetKey("asset2"),
            dependencies=[
                ExternalAssetDependency(upstream_asset_key=AssetKey("asset1"), input_name="asset1")
            ],
            depended_by=[],
            op_name="asset2",
            op_description=None,
            job_names=["assets_job"],
            output_name="result",
            output_description=None,
        ),
    ]


def test_two_downstream_assets_job():
    @asset
    def asset1():
        return 1

    @asset
    def asset2_a(asset1):
        assert asset1 == 1

    @asset
    def asset2_b(asset1):
        assert asset1 == 1

    assets_job = build_assets_job("assets_job", [asset1, asset2_a, asset2_b])
    external_asset_nodes = external_asset_graph_from_defs([assets_job], foreign_assets_by_key={})

    assert external_asset_nodes == [
        ExternalAssetNode(
            asset_key=AssetKey("asset1"),
            dependencies=[],
            depended_by=[
                ExternalAssetDependedBy(
                    downstream_asset_key=AssetKey("asset2_a"), input_name="asset1"
                ),
                ExternalAssetDependedBy(
                    downstream_asset_key=AssetKey("asset2_b"), input_name="asset1"
                ),
            ],
            op_name="asset1",
            op_description=None,
            job_names=["assets_job"],
            output_name="result",
            output_description=None,
        ),
        ExternalAssetNode(
            asset_key=AssetKey("asset2_a"),
            dependencies=[
                ExternalAssetDependency(upstream_asset_key=AssetKey("asset1"), input_name="asset1")
            ],
            depended_by=[],
            op_name="asset2_a",
            op_description=None,
            job_names=["assets_job"],
            output_name="result",
            output_description=None,
        ),
        ExternalAssetNode(
            asset_key=AssetKey("asset2_b"),
            dependencies=[
                ExternalAssetDependency(upstream_asset_key=AssetKey("asset1"), input_name="asset1")
            ],
            depended_by=[],
            op_name="asset2_b",
            op_description=None,
            job_names=["assets_job"],
            output_name="result",
            output_description=None,
        ),
    ]


def test_cross_job_asset_dependency():
    @asset
    def asset1():
        return 1

    @asset
    def asset2(asset1):
        assert asset1 == 1

    assets_job1 = build_assets_job("assets_job1", [asset1])
    assets_job2 = build_assets_job("assets_job2", [asset2], source_assets=[asset1])
    external_asset_nodes = external_asset_graph_from_defs(
        [assets_job1, assets_job2], foreign_assets_by_key={}
    )

    assert external_asset_nodes == [
        ExternalAssetNode(
            asset_key=AssetKey("asset1"),
            dependencies=[],
            depended_by=[
                ExternalAssetDependedBy(
                    downstream_asset_key=AssetKey("asset2"), input_name="asset1"
                )
            ],
            op_name="asset1",
            op_description=None,
            job_names=["assets_job1"],
            output_name="result",
            output_description=None,
        ),
        ExternalAssetNode(
            asset_key=AssetKey("asset2"),
            dependencies=[
                ExternalAssetDependency(upstream_asset_key=AssetKey("asset1"), input_name="asset1")
            ],
            depended_by=[],
            op_name="asset2",
            op_description=None,
            job_names=["assets_job2"],
            output_name="result",
            output_description=None,
        ),
    ]


def test_same_asset_in_multiple_pipelines():
    @asset
    def asset1():
        return 1

    job1 = build_assets_job("job1", [asset1])
    job2 = build_assets_job("job2", [asset1])

    external_asset_nodes = external_asset_graph_from_defs([job1, job2], foreign_assets_by_key={})

    assert external_asset_nodes == [
        ExternalAssetNode(
            asset_key=AssetKey("asset1"),
            dependencies=[],
            depended_by=[],
            op_name="asset1",
            op_description=None,
            job_names=["job1", "job2"],
            output_name="result",
            output_description=None,
        ),
    ]


def test_basic_multi_asset():
    @multi_asset(
        outs={
            f"out{i}": Out(description=f"foo: {i}", asset_key=AssetKey(f"asset{i}"))
            for i in range(10)
        }
    )
    def assets():
        pass

    assets_job = build_assets_job("assets_job", [assets])

    external_asset_nodes = external_asset_graph_from_defs([assets_job], foreign_assets_by_key={})

    assert external_asset_nodes == [
        ExternalAssetNode(
            asset_key=AssetKey(f"asset{i}"),
            dependencies=[],
            depended_by=[],
            op_name="assets",
            op_description=None,
            job_names=["assets_job"],
            output_name=f"out{i}",
            output_description=f"foo: {i}",
        )
        for i in range(10)
    ]


def test_inter_op_dependency():
    @asset
    def in1():
        pass

    @asset
    def in2():
        pass

    @asset
    def downstream(only_in, mixed, only_out):  # pylint: disable=unused-argument
        pass

    @multi_asset(
        outs={
            "only_in": Out(),
            "mixed": Out(in_deps=["in1"], out_deps=["only_in"]),
            "only_out": Out(in_deps=[], out_deps=["only_in", "mixed"]),
        }
    )
    def assets(in1, in2):  # pylint: disable=unused-argument
        pass

    assets_job = build_assets_job("assets_job", [in1, in2, assets, downstream])

    external_asset_nodes = external_asset_graph_from_defs([assets_job], foreign_assets_by_key={})
    # sort so that test is deterministic
    sorted_nodes = sorted(
        [
            node._replace(
                dependencies=sorted(node.dependencies, key=lambda d: d.upstream_asset_key),
                depended_by=sorted(node.depended_by, key=lambda d: d.downstream_asset_key),
            )
            for node in external_asset_nodes
        ],
        key=lambda n: n.asset_key,
    )

    assert sorted_nodes == [
        ExternalAssetNode(
            asset_key=AssetKey(["downstream"]),
            dependencies=[
                ExternalAssetDependency(upstream_asset_key=AssetKey(["mixed"]), input_name="mixed"),
                ExternalAssetDependency(
                    upstream_asset_key=AssetKey(["only_in"]), input_name="only_in"
                ),
                ExternalAssetDependency(
                    upstream_asset_key=AssetKey(["only_out"]), input_name="only_out"
                ),
            ],
            depended_by=[],
            op_name="downstream",
            op_description=None,
            job_names=["assets_job"],
            output_name="result",
        ),
        ExternalAssetNode(
            asset_key=AssetKey(["in1"]),
            dependencies=[],
            depended_by=[
                ExternalAssetDependedBy(downstream_asset_key=AssetKey(["mixed"]), input_name="in1"),
                ExternalAssetDependedBy(
                    downstream_asset_key=AssetKey(["only_in"]), input_name="in1"
                ),
            ],
            op_name="in1",
            op_description=None,
            job_names=["assets_job"],
            output_name="result",
        ),
        ExternalAssetNode(
            asset_key=AssetKey(["in2"]),
            dependencies=[],
            depended_by=[
                ExternalAssetDependedBy(
                    downstream_asset_key=AssetKey(["only_in"]), input_name="in2"
                )
            ],
            op_name="in2",
            op_description=None,
            job_names=["assets_job"],
            output_name="result",
        ),
        ExternalAssetNode(
            asset_key=AssetKey(["mixed"]),
            dependencies=[
                ExternalAssetDependency(upstream_asset_key=AssetKey(["in1"]), input_name="in1"),
                ExternalAssetDependency(
                    upstream_asset_key=AssetKey(["only_in"]), output_name="only_in"
                ),
            ],
            depended_by=[
                ExternalAssetDependedBy(
                    downstream_asset_key=AssetKey(["downstream"]), input_name="mixed"
                ),
                ExternalAssetDependedBy(
                    downstream_asset_key=AssetKey(["only_out"]), output_name="mixed"
                ),
            ],
            op_name="assets",
            op_description=None,
            job_names=["assets_job"],
            output_name="mixed",
        ),
        ExternalAssetNode(
            asset_key=AssetKey(["only_in"]),
            dependencies=[
                ExternalAssetDependency(upstream_asset_key=AssetKey(["in1"]), input_name="in1"),
                ExternalAssetDependency(upstream_asset_key=AssetKey(["in2"]), input_name="in2"),
            ],
            depended_by=[
                ExternalAssetDependedBy(
                    downstream_asset_key=AssetKey(["downstream"]), input_name="only_in"
                ),
                ExternalAssetDependedBy(
                    downstream_asset_key=AssetKey(["mixed"]), output_name="only_in"
                ),
                ExternalAssetDependedBy(
                    downstream_asset_key=AssetKey(["only_out"]), output_name="only_in"
                ),
            ],
            op_name="assets",
            op_description=None,
            job_names=["assets_job"],
            output_name="only_in",
        ),
        ExternalAssetNode(
            asset_key=AssetKey(["only_out"]),
            dependencies=[
                ExternalAssetDependency(
                    upstream_asset_key=AssetKey(["mixed"]), output_name="mixed"
                ),
                ExternalAssetDependency(
                    upstream_asset_key=AssetKey(["only_in"]), output_name="only_in"
                ),
            ],
            depended_by=[
                ExternalAssetDependedBy(
                    downstream_asset_key=AssetKey(["downstream"]), input_name="only_out"
                ),
            ],
            op_name="assets",
            op_description=None,
            job_names=["assets_job"],
            output_name="only_out",
        ),
    ]


"""
from collections import defaultdict
def external_asset_graph_from_defs(pipelines, foreign_assets_by_key):
    node_defs_by_asset_key = defaultdict(list)

    deps = defaultdict(dict)
    dep_by = defaultdict(list)
    all_upstream_asset_keys = set()

    for pipeline in pipelines:
        for node_def in pipeline.all_node_defs:
            node_asset_keys = set()
            for output_def in node_def.output_defs:
                asset_key = output_def.hardcoded_asset_key

                if asset_key:
                    node_asset_keys.add(asset_key)
                    node_defs_by_asset_key[asset_key].append((node_def, pipeline))

            for input_def in node_def.input_defs:
                upstream_asset_key = input_def.hardcoded_asset_key

                if upstream_asset_key:
                    all_upstream_asset_keys.add(upstream_asset_key)
                    for node_asset_key in node_asset_keys:
                        deps[node_asset_key][input_def.name] = ExternalAssetDependency(
                            upstream_asset_key=upstream_asset_key,
                            input_name=input_def.name,
                        )
                        dep_by[upstream_asset_key].append(
                            ExternalAssetDependedBy(
                                downstream_asset_key=node_asset_key,
                                input_name=input_def.name,
                            )
                        )

    asset_keys_without_definitions = all_upstream_asset_keys.difference(
        node_defs_by_asset_key.keys()
    ).difference(foreign_assets_by_key.keys())

    asset_nodes = [
        ExternalAssetNode(
            asset_key=asset_key,
            dependencies=list(deps[asset_key].values()),
            depended_by=dep_by[asset_key],
            job_names=[],
        )
        for asset_key in asset_keys_without_definitions
    ]

    for foreign_asset in foreign_assets_by_key.values():
        if foreign_asset.key in node_defs_by_asset_key:
            raise DagsterInvariantViolationError(
                f"Asset with key {foreign_asset.key.to_string()} is defined both as a foreign asset"
                " and as a non-foreign asset"
            )

        asset_nodes.append(
            ExternalAssetNode(
                asset_key=foreign_asset.key,
                dependencies=list(deps[foreign_asset.key].values()),
                depended_by=dep_by[foreign_asset.key],
                job_names=[],
                op_description=foreign_asset.description,
            )
        )

    for asset_key, node_tuple_list in node_defs_by_asset_key.items():
        node_def = node_tuple_list[0][0]
        job_names = [node_tuple[1].name for node_tuple in node_tuple_list]

        # temporary workaround to retrieve asset partition definition from job
        output = node_def.output_dict.get("result", None)
        partitions_def_data = None

        if output and output._asset_partitions_def:  # pylint: disable=protected-access
            partitions_def = output._asset_partitions_def  # pylint: disable=protected-access
            if partitions_def:
                pass

        asset_nodes.append(
            ExternalAssetNode(
                asset_key=asset_key,
                dependencies=list(deps[asset_key].values()),
                depended_by=dep_by[asset_key],
                op_name=node_def.name,
                op_description=node_def.description,
                job_names=job_names,
                partitions_def_data=partitions_def_data,
            )
        )

    return asset_nodes
    """


def test_source_not_foreign_asset():

    foo = ForeignAsset(key=AssetKey("foo"), description=None)

    @asset
    def bar(foo):
        pass

    assets_job = build_assets_job("assets_job", [bar], source_assets=[foo])

    external_asset_nodes = external_asset_graph_from_defs([assets_job], foreign_assets_by_key={})
    assert external_asset_nodes == [
        ExternalAssetNode(
            asset_key=AssetKey("foo"),
            op_description=None,
            dependencies=[],
            depended_by=[ExternalAssetDependedBy(AssetKey("bar"), input_name="foo")],
            job_names=[],
        ),
        ExternalAssetNode(
            asset_key=AssetKey("bar"),
            op_name="bar",
            op_description=None,
            dependencies=[ExternalAssetDependency(AssetKey("foo"), input_name="foo")],
            depended_by=[],
            job_names=["assets_job"],
            output_name="result",
        ),
    ]


def test_unused_foreign_asset():
    foo = ForeignAsset(key=AssetKey("foo"), description="abc")
    bar = ForeignAsset(key=AssetKey("bar"), description="def")

    external_asset_nodes = external_asset_graph_from_defs(
        [], foreign_assets_by_key={AssetKey("foo"): foo, AssetKey("bar"): bar}
    )
    assert external_asset_nodes == [
        ExternalAssetNode(
            asset_key=AssetKey("foo"),
            op_description="abc",
            dependencies=[],
            depended_by=[],
            job_names=[],
        ),
        ExternalAssetNode(
            asset_key=AssetKey("bar"),
            op_description="def",
            dependencies=[],
            depended_by=[],
            job_names=[],
        ),
    ]


def test_used_foreign_asset():
    bar = ForeignAsset(key=AssetKey("bar"), description="def")

    @asset
    def foo(bar):
        assert bar

    job1 = build_assets_job("job1", [foo], source_assets=[bar])

    external_asset_nodes = external_asset_graph_from_defs(
        [job1], foreign_assets_by_key={AssetKey("bar"): bar}
    )
    assert external_asset_nodes == [
        ExternalAssetNode(
            asset_key=AssetKey("bar"),
            op_description="def",
            dependencies=[],
            depended_by=[
                ExternalAssetDependedBy(downstream_asset_key=AssetKey(["foo"]), input_name="bar")
            ],
            job_names=[],
        ),
        ExternalAssetNode(
            asset_key=AssetKey("foo"),
            op_name="foo",
            op_description=None,
            dependencies=[
                ExternalAssetDependency(upstream_asset_key=AssetKey(["bar"]), input_name="bar")
            ],
            depended_by=[],
            job_names=["job1"],
            output_name="result",
            output_description=None,
        ),
    ]


def test_foreign_asset_conflicts_with_asset():
    bar_foreign_asset = ForeignAsset(key=AssetKey("bar"), description="def")

    @asset
    def bar():
        pass

    job1 = build_assets_job("job1", [bar])

    with pytest.raises(DagsterInvariantViolationError):
        external_asset_graph_from_defs(
            [job1], foreign_assets_by_key={AssetKey("bar"): bar_foreign_asset}
        )


def test_input_name_or_output_name_dep_by():
    with pytest.raises(CheckError, match="Exactly one"):
        ExternalAssetDependedBy(
            downstream_asset_key=AssetKey("bar"), input_name="foo", output_name="foo"
        )
    with pytest.raises(CheckError, match="Exactly one"):
        ExternalAssetDependedBy(
            downstream_asset_key=AssetKey("bar"), input_name=None, output_name=None
        )


def test_input_name_or_output_name_dependency():
    with pytest.raises(CheckError, match="Exactly one"):
        ExternalAssetDependency(
            upstream_asset_key=AssetKey("bar"), input_name="foo", output_name="foo"
        )
    with pytest.raises(CheckError, match="Exactly one"):
        ExternalAssetDependency(
            upstream_asset_key=AssetKey("bar"), input_name=None, output_name=None
        )


def test_back_compat_external_sensor():
    SERIALIZED_0_12_10_SENSOR = '{"__class__": "ExternalSensorData", "description": null, "min_interval": null, "mode": "default", "name": "my_sensor", "pipeline_name": "my_pipeline", "solid_selection": null}'
    external_sensor_data = deserialize_json_to_dagster_namedtuple(SERIALIZED_0_12_10_SENSOR)
    assert isinstance(external_sensor_data, ExternalSensorData)
    assert len(external_sensor_data.target_dict) == 1
    assert "my_pipeline" in external_sensor_data.target_dict
    target = external_sensor_data.target_dict["my_pipeline"]
    assert isinstance(target, ExternalTargetData)
    assert target.pipeline_name == "my_pipeline"
