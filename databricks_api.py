import os
from databricks_cli.sdk.api_client import ApiClient
from databricks_cli.dbfs.dbfs_path import DbfsPath
from databricks_cli.dbfs.api import DbfsApi
from typing import List

DBFS_PATH_DIR = "dbfs:/FileStore/tables"
SOURCE_PATH_DIR = "./data"


class DatabricksApi:

    def __init__(self, databricks_token: str, databricks_host: str) -> None:
        """Class' Constructor

        Parameters
        ----------
        databricks_token : str
            A Databricks personal access token for the Databricks workspace
        databricks_host : str
            Represents the workspace instance URL.
        """
        self.databricks_token = databricks_token
        self.databricks_host = databricks_host
        self.__set_api_client()

    def upload_files_to_dbfs(self, overwrite: bool = True) -> None:
        """Upload Files to DBFS

        This method creates into DBFS all files
        that exist in the SOURCE_PATH_DIR directory.

        Parameters
        ----------
        overwrite : bool, optional
            The flag that specifies whether to overwrite existing file,
            by default True
        """
        files_list = self.__get_files_path()

        for file in files_list:
            file_name = os.path.basename(file)
            dbfs_path = f"{DBFS_PATH_DIR}/{file_name}"
            src_path = f"{SOURCE_PATH_DIR}/{file_name}"
            self.upload_file_to_dbfs(dbfs_path, src_path, overwrite)

    def upload_file_to_dbfs(
                    self,
                    dbfs_path: str,
                    src_path: str,
                    overwrite: bool = True
                ) -> None:
        """Upload a File to DBFS

        This method uploads the file passed as a parameter
        and creates a new file into the DBFS.

        Parameters
        ----------
        dbfs_path : str
            The path of the new file.
            The path should be the absolute DBFS path (e.g. /mnt/foo/).
        src_path : str
            The source path of the file.
        overwrite : bool, optional
            The flag that specifies whether to overwrite existing file,
            by default True
        """
        dbfs_path = DbfsPath(dbfs_path)
        dbfs_api = DbfsApi(self.api_client)

        dbfs_api.put_file(
            dbfs_path=dbfs_path,
            src_path=src_path,
            overwrite=overwrite
        )

    def __set_api_client(self) -> None:
        """Set API Cliente

        This method enables the code to authenticate
        with the Databricks REST API.

        Link: https://docs.databricks.com/dev-tools/python-api.html \
            #step-2-write-your-code
        """
        self.api_client = ApiClient(
            host=self.databricks_host,
            token=self.databricks_token
        )

    def __get_files_path(self) -> List[str]:
        """Get Files Path

        This method returns a list of files' paths
        in the target_path directory.

        Returns
        -------
        List[str]
            List of files' paths.
        """
        target_path = SOURCE_PATH_DIR
        files_list = []

        for path in os.listdir(target_path):
            if os.path.isfile(os.path.join(target_path, path)):
                files_list.append(os.path.join(target_path, path))

        return files_list
