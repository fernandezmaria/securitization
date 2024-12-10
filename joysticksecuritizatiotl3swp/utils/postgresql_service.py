from alfred.database.managers import PostgreSQLManager
from alfred.patterns.singleton import SingletonServiceCreator


class PostgreSQLService(SingletonServiceCreator):
    """
    This class implements the Singleton pattern so that database connection is created only once
    """

    def __init__(
        self,
        db_name: str,
        execution_env: str,
        host: str,
        environ: str,
        country: str,
        port: str,
    ):
        self.pg_manager = PostgreSQLManager(
            db_name, execution_env, host, environ, country, port, conn_mode="write"
        )

    def write(self, data, table_name, save_mode="overwrite"):
        """
        This method is resposible for writing spark dataframes from postgresql using Alfred PosgresManager.

        :param data : str
            Spark dataframe
        :param table_name : str
            PostgreSQL Table Namel
        :param save_mode : str
            PostgreSQL save mode
        """
        self.pg_manager.write(data, table_name, save_mode, permissions="all")

    def read(self, table_name):
        """
        This method is resposible for reading spark dataframes from postgresql using Alfred PosgresManager.

        :param table_name : str
            PostgreSQL Table Namel
        :return: DataFrame
            Readed Dataframe
        """
        return self.pg_manager.read(table_name)

    def index_on(self, table_name, columns):
        """
        This method is resposible for generating an Index in a list of columns of type B-Tree in a table.

        :param table_name : str
            PostgreSQL Table Namel
        :param columns : List
            Columns of the table
        """
        self.pg_manager.create_indices(table_name, columns)

    def drop_index(self, table_name):
        """
        This method is resposible for droping all indices in a table.

        :param table_name : str
            PostgreSQL Table Name
        """
        self.pg_manager.drop_all_table_indices(table_name)

    def is_table_on_schema(self, schema_name, table_name):
        """
        This method is resposible for search if table exists
        :param schema : str
            PostgreSQL Schema Name
        :param table_name : str
            PostgreSQL Table Name
        """
        return table_name in self.pg_manager.list_tables(schema_name)

    def delete_table(self, table_name):
        """
        This method is resposible for deleting a table.

        :param table_name : str
            PostgreSQL Table Name
        """
        try:
            self.pg_manager.drop(table_name)
        except Exception:
            pass
