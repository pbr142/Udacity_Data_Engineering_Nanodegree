from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadDimensionOperator(BaseOperator):

    ui_color = '#80BD9E'
    truncate_stmt = """
        TRUNCATE TABLE {table}
    """
    insert_into_stmt = """
        INSERT INTO {table} 
        {query}
    """

    @apply_defaults
    def __init__(self,
                 redshift_conn_id,
                 table,
                 query,
                 truncate=False,
                 *args, **kwargs):

        super(LoadDimensionOperator, self).__init__(*args, **kwargs)

        self.redshift_conn_id = redshift_conn_id
        self.table = table
        self.query = query
        self.truncate = truncate

    def execute(self, context):
        redshift_hook = PostgresHook(postgres_conn_id=self.redshift_conn_id)

        if self.truncate:
            self.log.info("Truncate table")
            redshift_hook.run(LoadDimensionOperator.truncate_stmt.format(
                table=self.table
            ))

        self.log.info("Insert dimension table data")
        redshift_hook.run(LoadDimensionOperator.insert_into_stmt.format(
            table=self.table,
            query=self.query
        ))
		self.log.info("Dimension table successfully inserted")