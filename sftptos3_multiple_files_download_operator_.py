from __future__ import annotations
import os
import re

from tempfile import NamedTemporaryFile
from typing import TYPE_CHECKING, Sequence
from urllib.parse import urlsplit
from airflow.exceptions import AirflowException

from airflow.models import BaseOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.ssh.hooks.ssh import SSHHook
from typing import Any
import logging

if TYPE_CHECKING:
    from airflow.utils.context import Context

class SFTPToS3MultipleFilesDownloadOperator(BaseOperator):
    """
    Transfer files from an SFTP server to Amazon S3.

    .. seealso::
        For more information on how to use this operator, take a look at the guide:
        :ref:`howto/operator:SFTPToS3Operator`

    :param sftp_conn_id: The sftp connection id. The name or identifier for
        establishing a connection to the SFTP server.
    :param sftp_path: The sftp remote path. This is the specified file path
        for downloading the file from the SFTP server.
    :param s3_conn_id: The s3 connection id. The name or identifier for
        establishing a connection to S3
    :param s3_bucket: The targeted s3 bucket. This is the S3 bucket to where
        the file is uploaded.
    :param s3_key: The targeted s3 key. This is the specified path for
        uploading the file to S3.
    :param use_temp_file: If True, copies file first to local,
        if False streams file from SFTP to S3.
    """

    template_fields = ('remote_filename_pattern','filetype', 'remote_host')

    def __init__(
            self,
            *,
            ssh_hook=None,
            ssh_conn_id=None,
            remote_host=None,
            remote_filepath=None,
            remote_filelist=None,
            s3_bucket: str,
            s3_key: str,
            # sftp_path: str,
            # sftp_conn_id: str = "ssh_default",
            s3_conn_id: str = "aws_default",
            use_temp_file: bool = False,

            **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.ssh_hook = ssh_hook
        self.ssh_conn_id = ssh_conn_id
        self.remote_host = remote_host
        self.remote_filepath = remote_filepath
        self.remote_filelist = remote_filelist
        self.s3_bucket = s3_bucket
        self.s3_key = s3_key
        self.s3_conn_id = s3_conn_id
        self.use_temp_file = use_temp_file

    @staticmethod
    def get_s3_key(s3_key: str) -> str:
        """Parse the correct format for S3 keys regardless of how the S3 url is passed."""
        parsed_s3_key = urlsplit(s3_key)
        return parsed_s3_key.path.lstrip("/")

    def execute(self, context: Any) -> str:
        file_msg = None
        try:
            if self.ssh_conn_id:
                if self.ssh_hook and isinstance(self.ssh_hook, SSHHook):
                    self.log.info("ssh_conn_id is ignored when ssh_hook is provided.")
                else:
                    self.log.info(
                        "ssh_hook is not provided or invalid. Trying ssh_conn_id to create SSHHook."
                    )
                    self.ssh_hook = SSHHook(ssh_conn_id=self.ssh_conn_id)

            if not self.ssh_hook:
                raise AirflowException("Cannot operate without ssh_hook or ssh_conn_id.")

            if self.remote_host is not None:
                self.log.info(
                    "remote_host is provided explicitly. "
                    "It will replace the remote_host which was defined "
                    "in ssh_hook or predefined in connection of ssh_conn_id."
                )
                self.ssh_hook.remote_host = self.remote_host

            with self.ssh_hook.get_conn() as ssh_client:
                sftp_client = ssh_client.open_sftp()


                self.s3_key = self.get_s3_key(self.s3_key)
                self.log.info(f"self.remote_filepath: {self.remote_filepath}, self.s3_key: {self.s3_key}")
                s3_hook = S3Hook(self.s3_conn_id)

                self.log.info(f"download filelist: {self.remote_filelist}")

                for idx, filename in enumerate(self.remote_filelist):
                    sftp_file_fullpath = os.path.join(self.remote_filepath, filename)
                    s3_key = f"{self.s3_key}/{filename}"
                    self.log.info(f"{idx+1} th file name: {filename} start to transfer from sftp://{sftp_file_fullpath} to s3://{self.s3_bucket}/{self.s3_key}")

                    if self.use_temp_file:
                        with NamedTemporaryFile("w+b", delete=True) as f:
                            logging.info("temp area")
                            self.log.info(f"sftp_file_fullpath: {sftp_file_fullpath}\n"
                                         f"f.name: {f.name}")
                            sftp_client.get(sftp_file_fullpath, f.name)


                            s3_hook.load_file(filename=f.name, key=s3_key, bucket_name=self.s3_bucket, replace=True)
                            logging.info(f"f.name: {f.name}, type(f.name): {type(f.name)}\n"
                                         f"s3_key: {s3_key}, type(s3_key): {type(s3_key)}")
                    else:
                        logging.info("NOT temp area")
                        logging.info(f"sftp_file_fullpath: {sftp_file_fullpath}")
                        with sftp_client.file(sftp_file_fullpath, mode="rb") as data:
                            s3_hook.get_conn().upload_fileobj(data, self.s3_bucket, s3_key, Callback=self.log.info)
                            self.log.info(f"data: {data}, type(data): {type(data)}, \n"
                                  f"self.s3_bucket: {self.s3_bucket}, type(self.s3_bucket): {type(self.s3_bucket)}, \n"
                                  f"self.s3_key:{s3_key}, type(self.s3_key):{type(s3_key)}")

        except Exception as e:
            raise AirflowException(f"Error while transferring {file_msg}, error: {str(e)}")

        return self.remote_filepath
