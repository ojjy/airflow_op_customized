import os
from airflow.exceptions import AirflowException
from airflow.models import BaseOperator
from airflow.providers.ssh.hooks.ssh import SSHHook
from typing import Any
import re

# 정규표현식에 맞는 파일 명 혹은 파일 타입을 찾아 matching되는 파일들 모두 다운로드 한다
class SFTPMultipleFilesDownloadOperator(BaseOperator):
    template_fields = ('local_directory','remote_filename_pattern','filetype', 'remote_host')

    def __init__(
            self,
            *,
            ssh_hook=None,
            ssh_conn_id=None,
            remote_host=None,
            local_directory=None,
            remote_filepath=None,
            remote_filename_pattern=None,
            filetype=None,
            confirm=True,
            **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.ssh_hook = ssh_hook
        self.ssh_conn_id = ssh_conn_id
        self.remote_host = remote_host
        self.local_directory = local_directory
        self.filetype = filetype
        self.remote_filepath = remote_filepath
        self.remote_filename_pattern = remote_filename_pattern
        self.confirm = confirm
        # self.create_intermediate_dirs = create_intermediate_dirs

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
                all_files = sftp_client.listdir(path=self.remote_filepath)
                print(all_files)
                self.log.info(f'Found {len(all_files)} files on server')

                # filelist with specific filetype
                regex = re.compile(f'.*{self.filetype}$')
                filelist_specific_type = [file_specific_type for file_specific_type in all_files if
                                          regex.match(file_specific_type)]

                # filelist with specific filename among specific filetype
                regex = re.compile(self.remote_filename_pattern)
                filelist_specific_pattern = [file_specific_pattern for file_specific_pattern in filelist_specific_type
                                             if regex.match(file_specific_pattern)]
                filelist_specific_pattern=filelist_specific_pattern[:2]
                for f in filelist_specific_pattern:
                    self.log.info(f"Starting to transfer from /{f} to {self.local_directory}/{f}")
                    sftp_client.get(f'/{os.path.join(self.remote_filepath,f)}', f'{self.local_directory}/{f}')

        except Exception as e:
            raise AirflowException(f"Error while transferring {file_msg}, error: {str(e)}")

        return self.local_directory