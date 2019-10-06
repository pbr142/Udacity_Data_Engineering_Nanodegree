from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadFactOperator(BaseOperator):

    ui_color = '#F98866'
    insert_into_stmt = """
        INSERT INTO {table} 
        {query}
    """

    @apply_defaults
    def __init__(self,
                 redshift_conn_id,
                 table,
                 query,
                 *args, **kwargs):

        super(LoadFactOperator, self).__init__(*args, **kwargs)

        self.redshift_conn_id = redshift_conn_id
        self.table = table
        self.query = query

    def execute(self, context):
		redshift_hook = PostgresHook(postgres_conn_id=self.redshift_conn_id)

        redshift_hook.run(LoadFactOperator.insert_into_stmt.format(
            table=self.table,
            query=self.query
        ))