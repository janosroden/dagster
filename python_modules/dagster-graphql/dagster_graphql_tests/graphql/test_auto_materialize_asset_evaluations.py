from datetime import datetime

import dagster._check as check
import pendulum
from dagster import AssetKey
from dagster._core.definitions.asset_daemon_cursor import AssetDaemonCursor
from dagster._core.definitions.auto_materialize_rule import (
    AutoMaterializeAssetEvaluation,
    AutoMaterializeRule,
    AutoMaterializeRuleEvaluation,
    ParentUpdatedRuleEvaluationData,
    WaitingOnAssetsRuleEvaluationData,
)
from dagster._core.definitions.partition import (
    SerializedPartitionsSubset,
)
from dagster._core.definitions.run_request import (
    InstigatorType,
)
from dagster._core.definitions.time_window_partitions import TimeWindowPartitionsDefinition
from dagster._core.scheduler.instigation import (
    TickData,
    TickStatus,
)
from dagster._core.workspace.context import WorkspaceRequestContext
from dagster._daemon.asset_daemon import (
    CURSOR_KEY,
    FIXED_AUTO_MATERIALIZATION_INSTIGATOR_NAME,
    FIXED_AUTO_MATERIALIZATION_ORIGIN_ID,
    FIXED_AUTO_MATERIALIZATION_SELECTOR_ID,
)
from dagster_graphql.test.utils import execute_dagster_graphql

from dagster_graphql_tests.graphql.graphql_context_test_suite import (
    ExecutingGraphQLContextTestMatrix,
)
from dagster_graphql_tests.graphql.repo import static_partitions_def

TICKS_QUERY = """
query AssetDameonTicksQuery($dayRange: Int, $dayOffset: Int, $statuses: [InstigationTickStatus!], $limit: Int, $cursor: String) {
    autoMaterializeTicks(dayRange: $dayRange, dayOffset: $dayOffset, statuses: $statuses, limit: $limit, cursor: $cursor) {
        id
        timestamp
        status
    }
}
"""


def _create_tick(instance, status, timestamp):
    return instance.create_tick(
        TickData(
            instigator_origin_id=FIXED_AUTO_MATERIALIZATION_ORIGIN_ID,
            instigator_name=FIXED_AUTO_MATERIALIZATION_INSTIGATOR_NAME,
            instigator_type=InstigatorType.AUTO_MATERIALIZE,
            status=status,
            timestamp=timestamp,
            selector_id=FIXED_AUTO_MATERIALIZATION_SELECTOR_ID,
            run_ids=[],
        )
    )


class TestAutoMaterializeTicks(ExecutingGraphQLContextTestMatrix):
    def test_get_tick_range(self, graphql_context):
        result = execute_dagster_graphql(
            graphql_context,
            TICKS_QUERY,
            variables={"dayRange": None, "dayOffset": None},
        )
        assert len(result.data["autoMaterializeTicks"]) == 0

        now = pendulum.now("UTC")

        success_1 = _create_tick(graphql_context.instance, TickStatus.SUCCESS, now.timestamp())

        success_2 = _create_tick(
            graphql_context.instance, TickStatus.SUCCESS, now.subtract(days=1, hours=1).timestamp()
        )

        _create_tick(
            graphql_context.instance, TickStatus.SKIPPED, now.subtract(days=2, hours=1).timestamp()
        )

        result = execute_dagster_graphql(
            graphql_context,
            TICKS_QUERY,
            variables={"dayRange": None, "dayOffset": None},
        )
        assert len(result.data["autoMaterializeTicks"]) == 3

        result = execute_dagster_graphql(
            graphql_context,
            TICKS_QUERY,
            variables={"dayRange": 1, "dayOffset": None},
        )
        assert len(result.data["autoMaterializeTicks"]) == 1

        result = execute_dagster_graphql(
            graphql_context,
            TICKS_QUERY,
            variables={"dayRange": None, "dayOffset": None, "statuses": ["SUCCESS"]},
        )
        assert len(result.data["autoMaterializeTicks"]) == 2

        result = execute_dagster_graphql(
            graphql_context,
            TICKS_QUERY,
            variables={"dayRange": None, "dayOffset": None, "statuses": ["SUCCESS"], "limit": 1},
        )
        ticks = result.data["autoMaterializeTicks"]
        assert len(ticks) == 1
        assert ticks[0]["timestamp"] == success_1.timestamp

        cursor = ticks[0]["id"]

        result = execute_dagster_graphql(
            graphql_context,
            TICKS_QUERY,
            variables={
                "dayRange": None,
                "dayOffset": None,
                "statuses": ["SUCCESS"],
                "limit": 1,
                "cursor": cursor,
            },
        )
        ticks = result.data["autoMaterializeTicks"]
        assert len(ticks) == 1
        assert ticks[0]["timestamp"] == success_2.timestamp


QUERY = """
query GetEvaluationsQuery($assetKey: AssetKeyInput!, $limit: Int!, $cursor: String) {
    autoMaterializeAssetEvaluationsOrError(assetKey: $assetKey, limit: $limit, cursor: $cursor) {
        ... on AutoMaterializeAssetEvaluationRecords {
            records {
                numRequested
                numSkipped
                numDiscarded
                rulesWithRuleEvaluations {
                    rule {
                        decisionType
                    }
                    ruleEvaluations {
                        partitionKeysOrError {
                            ... on PartitionKeys {
                                partitionKeys
                            }
                            ... on Error {
                                message
                            }
                        }
                        evaluationData {
                            ... on TextRuleEvaluationData {
                                text
                            }
                            ... on ParentMaterializedRuleEvaluationData {
                                updatedAssetKeys {
                                    path
                                }
                                willUpdateAssetKeys {
                                    path
                                }
                            }
                            ... on WaitingOnKeysRuleEvaluationData {
                                waitingOnAssetKeys {
                                    path
                                }
                            }
                        }
                    }
                }
                rules {
                    decisionType
                    description
                    className
                }
            }
            currentEvaluationId
        }
    }
}
"""


class TestAutoMaterializeAssetEvaluations(ExecutingGraphQLContextTestMatrix):
    def test_get_historic_rules(self, graphql_context: WorkspaceRequestContext):
        check.not_none(
            graphql_context.instance.schedule_storage
        ).add_auto_materialize_asset_evaluations(
            evaluation_id=10,
            asset_evaluations=[
                AutoMaterializeAssetEvaluation(
                    asset_key=AssetKey("asset_one"),
                    partition_subsets_by_condition=[],
                    num_requested=0,
                    num_skipped=0,
                    num_discarded=0,
                    rule_snapshots=None,
                ),
                AutoMaterializeAssetEvaluation(
                    asset_key=AssetKey("asset_two"),
                    partition_subsets_by_condition=[],
                    num_requested=1,
                    num_skipped=0,
                    num_discarded=0,
                    rule_snapshots=[AutoMaterializeRule.materialize_on_missing().to_snapshot()],
                ),
            ],
        )

        results = execute_dagster_graphql(
            graphql_context,
            QUERY,
            variables={"assetKey": {"path": ["asset_one"]}, "limit": 10, "cursor": None},
        )
        assert len(results.data["autoMaterializeAssetEvaluationsOrError"]["records"]) == 1
        assert results.data["autoMaterializeAssetEvaluationsOrError"]["records"][0]["rules"] is None

        results = execute_dagster_graphql(
            graphql_context,
            QUERY,
            variables={"assetKey": {"path": ["asset_two"]}, "limit": 10, "cursor": None},
        )
        assert len(results.data["autoMaterializeAssetEvaluationsOrError"]["records"]) == 1
        assert (
            len(results.data["autoMaterializeAssetEvaluationsOrError"]["records"][0]["rules"]) == 1
        )
        rule = results.data["autoMaterializeAssetEvaluationsOrError"]["records"][0]["rules"][0]

        assert rule["decisionType"] == "MATERIALIZE"
        assert rule["description"] == "materialization is missing"
        assert rule["className"] == "MaterializeOnMissingRule"

    def _test_get_evaluations(self, graphql_context: WorkspaceRequestContext):
        results = execute_dagster_graphql(
            graphql_context,
            QUERY,
            variables={"assetKey": {"path": ["foo"]}, "limit": 10, "cursor": None},
        )
        assert results.data == {
            "autoMaterializeAssetEvaluationsOrError": {"records": [], "currentEvaluationId": None}
        }

        check.not_none(
            graphql_context.instance.schedule_storage
        ).add_auto_materialize_asset_evaluations(
            evaluation_id=10,
            asset_evaluations=[
                AutoMaterializeAssetEvaluation(
                    asset_key=AssetKey("asset_one"),
                    partition_subsets_by_condition=[],
                    num_requested=0,
                    num_skipped=0,
                    num_discarded=0,
                ),
                AutoMaterializeAssetEvaluation(
                    asset_key=AssetKey("asset_two"),
                    partition_subsets_by_condition=[
                        (
                            AutoMaterializeRuleEvaluation(
                                rule_snapshot=AutoMaterializeRule.materialize_on_missing().to_snapshot(),
                                evaluation_data=None,
                            ),
                            None,
                        )
                    ],
                    num_requested=1,
                    num_skipped=0,
                    num_discarded=0,
                ),
                AutoMaterializeAssetEvaluation(
                    asset_key=AssetKey("asset_three"),
                    partition_subsets_by_condition=[
                        (
                            AutoMaterializeRuleEvaluation(
                                rule_snapshot=AutoMaterializeRule.skip_on_parent_outdated().to_snapshot(),
                                evaluation_data=WaitingOnAssetsRuleEvaluationData(
                                    waiting_on_asset_keys=frozenset([AssetKey("asset_two")])
                                ),
                            ),
                            None,
                        )
                    ],
                    num_requested=0,
                    num_skipped=1,
                    num_discarded=0,
                ),
                AutoMaterializeAssetEvaluation(
                    asset_key=AssetKey("asset_four"),
                    partition_subsets_by_condition=[
                        (
                            AutoMaterializeRuleEvaluation(
                                rule_snapshot=AutoMaterializeRule.materialize_on_parent_updated().to_snapshot(),
                                evaluation_data=ParentUpdatedRuleEvaluationData(
                                    updated_asset_keys=frozenset([AssetKey("asset_two")]),
                                    will_update_asset_keys=frozenset([AssetKey("asset_three")]),
                                ),
                            ),
                            None,
                        )
                    ],
                    num_requested=1,
                    num_skipped=0,
                    num_discarded=0,
                ),
            ],
        )

        results = execute_dagster_graphql(
            graphql_context,
            QUERY,
            variables={"assetKey": {"path": ["asset_one"]}, "limit": 10, "cursor": None},
        )
        assert results.data == {
            "autoMaterializeAssetEvaluationsOrError": {
                "records": [
                    {
                        "numRequested": 0,
                        "numSkipped": 0,
                        "numDiscarded": 0,
                        "rulesWithRuleEvaluations": [],
                    }
                ],
                "currentEvaluationId": None,
            }
        }

        results = execute_dagster_graphql(
            graphql_context,
            QUERY,
            variables={"assetKey": {"path": ["asset_two"]}, "limit": 10, "cursor": None},
        )
        assert results.data == {
            "autoMaterializeAssetEvaluationsOrError": {
                "records": [
                    {
                        "numRequested": 1,
                        "numSkipped": 0,
                        "numDiscarded": 0,
                        "rulesWithRuleEvaluations": [
                            {
                                "rule": {"decisionType": "MATERIALIZE"},
                                "ruleEvaluations": [
                                    {
                                        "evaluationData": None,
                                        "partitionKeysOrError": None,
                                    }
                                ],
                            }
                        ],
                    }
                ],
                "currentEvaluationId": None,
            }
        }

        results = execute_dagster_graphql(
            graphql_context,
            QUERY,
            variables={"assetKey": {"path": ["asset_three"]}, "limit": 10, "cursor": None},
        )
        assert results.data == {
            "autoMaterializeAssetEvaluationsOrError": {
                "records": [
                    {
                        "numRequested": 0,
                        "numSkipped": 1,
                        "numDiscarded": 0,
                        "rulesWithRuleEvaluations": [
                            {
                                "rule": {"decisionType": "SKIP"},
                                "ruleEvaluations": [
                                    {
                                        "evaluationData": {
                                            "waitingOnAssetKeys": [{"path": ["asset_two"]}],
                                        },
                                        "partitionKeysOrError": None,
                                    }
                                ],
                            }
                        ],
                    }
                ],
                "currentEvaluationId": None,
            }
        }

        results = execute_dagster_graphql(
            graphql_context,
            QUERY,
            variables={"assetKey": {"path": ["asset_four"]}, "limit": 10, "cursor": None},
        )
        assert results.data == {
            "autoMaterializeAssetEvaluationsOrError": {
                "records": [
                    {
                        "numRequested": 1,
                        "numSkipped": 0,
                        "numDiscarded": 0,
                        "rulesWithRuleEvaluations": [
                            {
                                "rule": {"decisionType": "MATERIALIZE"},
                                "ruleEvaluations": [
                                    {
                                        "evaluationData": {
                                            "updatedAssetKeys": [{"path": ["asset_two"]}],
                                            "willUpdateAssetKeys": [{"path": ["asset_three"]}],
                                        },
                                        "partitionKeysOrError": None,
                                    }
                                ],
                            }
                        ],
                    }
                ],
                "currentEvaluationId": None,
            }
        }

    def _test_get_evaluations_with_partitions(self, graphql_context: WorkspaceRequestContext):
        results = execute_dagster_graphql(
            graphql_context,
            QUERY,
            variables={
                "assetKey": {"path": ["upstream_static_partitioned_asset"]},
                "limit": 10,
                "cursor": None,
            },
        )
        assert results.data == {
            "autoMaterializeAssetEvaluationsOrError": {"records": [], "currentEvaluationId": None}
        }

        check.not_none(
            graphql_context.instance.schedule_storage
        ).add_auto_materialize_asset_evaluations(
            evaluation_id=10,
            asset_evaluations=[
                AutoMaterializeAssetEvaluation(
                    asset_key=AssetKey("upstream_static_partitioned_asset"),
                    partition_subsets_by_condition=[
                        (
                            AutoMaterializeRuleEvaluation(
                                rule_snapshot=AutoMaterializeRule.materialize_on_missing().to_snapshot(),
                                evaluation_data=None,
                            ),
                            SerializedPartitionsSubset.from_subset(
                                static_partitions_def.empty_subset().with_partition_keys(
                                    ["a", "b"]
                                ),
                                static_partitions_def,
                                None,  # type: ignore
                            ),
                        )
                    ],
                    num_requested=2,
                    num_skipped=0,
                    num_discarded=0,
                ),
            ],
        )

        results = execute_dagster_graphql(
            graphql_context,
            QUERY,
            variables={
                "assetKey": {"path": ["upstream_static_partitioned_asset"]},
                "limit": 10,
                "cursor": None,
            },
        )

        assert len(results.data["autoMaterializeAssetEvaluationsOrError"]["records"]) == 1
        assert (
            results.data["autoMaterializeAssetEvaluationsOrError"]["records"][0]["numRequested"]
        ) == 2
        assert (
            results.data["autoMaterializeAssetEvaluationsOrError"]["records"][0]["numSkipped"]
        ) == 0
        assert (
            results.data["autoMaterializeAssetEvaluationsOrError"]["records"][0]["numDiscarded"]
        ) == 0
        assert (
            results.data["autoMaterializeAssetEvaluationsOrError"]["records"][0][
                "rulesWithRuleEvaluations"
            ][0]["rule"]["decisionType"]
        ) == "MATERIALIZE"
        assert set(
            results.data["autoMaterializeAssetEvaluationsOrError"]["records"][0][
                "rulesWithRuleEvaluations"
            ][0]["ruleEvaluations"][0]["partitionKeysOrError"]["partitionKeys"]
        ) == {"a", "b"}

    def _test_get_evaluations_invalid_partitions(self, graphql_context: WorkspaceRequestContext):
        wrong_partitions_def = TimeWindowPartitionsDefinition(
            cron_schedule="0 0 * * *", start=datetime(year=2020, month=1, day=5), fmt="%Y-%m-%d"
        )

        check.not_none(
            graphql_context.instance.schedule_storage
        ).add_auto_materialize_asset_evaluations(
            evaluation_id=10,
            asset_evaluations=[
                AutoMaterializeAssetEvaluation(
                    asset_key=AssetKey("upstream_static_partitioned_asset"),
                    partition_subsets_by_condition=[
                        (
                            AutoMaterializeRuleEvaluation(
                                rule_snapshot=AutoMaterializeRule.materialize_on_missing().to_snapshot(),
                                evaluation_data=None,
                            ),
                            SerializedPartitionsSubset.from_subset(
                                wrong_partitions_def.empty_subset().with_partition_keys(
                                    ["2023-07-07"]
                                ),
                                wrong_partitions_def,
                                None,  # type: ignore
                            ),
                        )
                    ],
                    num_requested=2,
                    num_skipped=0,
                    num_discarded=0,
                ),
            ],
        )

        results = execute_dagster_graphql(
            graphql_context,
            QUERY,
            variables={
                "assetKey": {"path": ["upstream_static_partitioned_asset"]},
                "limit": 10,
                "cursor": None,
            },
        )
        assert results.data == {
            "autoMaterializeAssetEvaluationsOrError": {
                "records": [
                    {
                        "numRequested": 2,
                        "numSkipped": 0,
                        "numDiscarded": 0,
                        "rulesWithRuleEvaluations": [
                            {
                                "rule": {"decisionType": "MATERIALIZE"},
                                "ruleEvaluations": [
                                    {
                                        "evaluationData": None,
                                        "partitionKeysOrError": {
                                            "message": (
                                                "Partition subset cannot be deserialized. The"
                                                " PartitionsDefinition may have changed."
                                            )
                                        },
                                    }
                                ],
                            },
                        ],
                    }
                ],
                "currentEvaluationId": None,
            }
        }

    def _test_current_evaluation_id(self, graphql_context: WorkspaceRequestContext):
        graphql_context.instance.daemon_cursor_storage.set_cursor_values(
            {CURSOR_KEY: AssetDaemonCursor.empty().serialize()}
        )

        results = execute_dagster_graphql(
            graphql_context,
            QUERY,
            variables={"assetKey": {"path": ["asset_two"]}, "limit": 10, "cursor": None},
        )
        assert results.data == {
            "autoMaterializeAssetEvaluationsOrError": {
                "records": [],
                "currentEvaluationId": 0,
            }
        }

        graphql_context.instance.daemon_cursor_storage.set_cursor_values(
            {
                CURSOR_KEY: (
                    AssetDaemonCursor.empty()
                    .with_updates(0, set(), set(), set(), {}, 42, None, [], 0)  # type: ignore
                    .serialize()
                )
            }
        )

        results = execute_dagster_graphql(
            graphql_context,
            QUERY,
            variables={"assetKey": {"path": ["asset_two"]}, "limit": 10, "cursor": None},
        )
        assert results.data == {
            "autoMaterializeAssetEvaluationsOrError": {
                "records": [],
                "currentEvaluationId": 42,
            }
        }
