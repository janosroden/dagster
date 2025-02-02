import sys

from dagster_pipes import init_dagster_pipes


class SomeSqlClient:
    def query(self, query_str: str) -> None:
        sys.stderr.write(f'Querying "{query_str}"\n')


if __name__ == "__main__":
    sql = sys.argv[1]

    context = init_dagster_pipes()

    client = SomeSqlClient()
    client.query(sql)
    context.report_asset_materialization(metadata={"sql": sql})
    context.log.info(f"Ran {sql}")
