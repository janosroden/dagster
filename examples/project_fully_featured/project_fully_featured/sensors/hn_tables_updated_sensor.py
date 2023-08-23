import json

from dagster import (
    AssetKey,
    AssetRecordsFilter,
    RunRequest,
    SensorDefinition,
    sensor,
)


def make_hn_tables_updated_sensor(job) -> SensorDefinition:
    """Returns a sensor that launches the given job when the HN "comments" and "stories" tables have
    both been updated.
    """

    @sensor(name=f"{job.name}_on_hn_tables_updated", job=job)
    def hn_tables_updated_sensor(context):
        cursor_dict = json.loads(context.cursor) if context.cursor else {}
        comments_cursor = cursor_dict.get("comments")
        stories_cursor = cursor_dict.get("stories")

        comments_event_records = context.instance.get_materialization_records(
            asset_key=AssetKey(["snowflake", "core", "comments"]),
            asset_records_filter=AssetRecordsFilter(after_cursor=comments_cursor),
            limit=1,
        )
        stories_event_records = context.instance.get_materialization_records(
            asset_key=AssetKey(["snowflake", "core", "stories"]),
            asset_records_filter=AssetRecordsFilter(after_cursor=stories_cursor),
            limit=1,
        )

        if not comments_event_records or not stories_event_records:
            return

        # make sure we only generate events if both table_a and table_b have been materialized since
        # the last evaluation.
        yield RunRequest(run_key=None)

        # update the sensor cursor by combining the individual event cursors from the two separate
        # asset event streams
        context.update_cursor(
            json.dumps(
                {
                    "comments": comments_event_records[0].storage_id,
                    "stories": stories_event_records[0].storage_id,
                }
            )
        )

    return hn_tables_updated_sensor
