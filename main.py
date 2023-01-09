from databricks_api import DatabricksApi
from decouple import config


def main():
    DATABRICKS_HOST = config("DATABRICKS_HOST")
    DATABRICKS_TOKEN = config("DATABRICKS_TOKEN")

    dbk = DatabricksApi(DATABRICKS_TOKEN, DATABRICKS_HOST)
    dbk.upload_files_to_dbfs()


if __name__ == "__main__":
    main()
