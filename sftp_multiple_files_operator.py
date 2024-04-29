import os
from airflow.exceptions import AirflowException, AirflowProviderDeprecationWarning
from airflow.models import BaseOperator
from airflow.providers.ssh.hooks.ssh import SSHHook
from airflow.providers.sftp.hooks.sftp import SFTPHook
from typing import Any, Tuple
import re
import warnings


class SFTPOperation:
    """Operation that can be used with SFTP."""

    PUT = "put"
    GET = "get"


# 정규표현식에 맞는 파일 명 혹은 파일 타입을 찾아 matching되는 파일들 모두 다운로드 한다
class SFTPMultipleFilesDownloadOperator(BaseOperator):
    template_fields = ('local_folderpath, remote_folderpath','remote_host')

    def __init__(
            self,
            *,
            ssh_hook : SSHHook | None = None,
            sftp_hook : SFTPHook | None = None,
            ssh_conn_id : str | None = None,
            remote_host : str | None = None,
            local_folderpath : str | None = None,
            local_filelist: list[str] | None=None,
            remote_filename_pattern: str | None=None,
            remote_folderpath: str| None=None,
            remote_filelist: list[str] | None=None,
            filetype: str | None = None,
            operation: str = SFTPOperation.GET,
            confirm: bool =True,
            create_intermediate_dirs: bool = False,
            **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.ssh_hook = ssh_hook
        self.sftp_hook = sftp_hook
        self.ssh_conn_id = ssh_conn_id
        self.remote_host = remote_host
        self.local_folderpath = local_folderpath
        self.local_filelist = local_filelist
        self.remote_folderpath = remote_folderpath
        self.remote_filelist = remote_filelist
        if filetype is not None:
            self.filetype = filetype
        self.operation = operation
        self.remote_filename_pattern = remote_filename_pattern
        self.confirm = confirm
        self.create_intermediate_dirs = create_intermediate_dirs

    def execute(self, context: Any) -> tuple[str | None, str | None, str]:

        if self.operation.lower() not in (SFTPOperation.GET, SFTPOperation.PUT):
            raise TypeError(
                f"Unsupported operation value {self.operation}, "
                f"expected {SFTPOperation.GET} or {SFTPOperation.PUT}."
            )
        if self.ssh_hook is not None:
            if not isinstance(self.ssh_hook, SSHHook):
                self.log.info("ssh_hook is invalid. Trying ssh_conn_id to create SFTPHook.")
                self.sftp_hook = SFTPHook(ssh_conn_id=self.ssh_conn_id)
            if self.sftp_hook is None:
                warnings.warn(
                    "Parameter `ssh_hook` is deprecated. "
                    "Please use `sftp_hook` instead. "
                    "The old parameter `ssh_hook` will be removed in a future version.",
                    AirflowProviderDeprecationWarning,
                    stacklevel=2,
                )
                self.sftp_hook = SFTPHook(ssh_hook=self.ssh_hook)

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

            # get - download remote-> local
            # filepath list -> sftp와 동일
            # folderpath remote folderpathd에서 pattern 맞는거 다운로드

            # put - upload remote <- local
            # filepath list -> sftp와 동일
            # folderpath local folder path에서 pattern에 맞는거
            # remote folder path로 업로드
            with self.ssh_hook.get_conn() as ssh_client:
                sftp_client = ssh_client.open_sftp()

                if self.operation.lower() == SFTPOperation.GET: # download
                    all_files = sftp_client.listdir(path=self.remote_folderpath)
                    print(all_files)
                    self.log.info(f'Found {len(all_files)} files on server')
                    if self.remote_filelist is not None:
                        for f in self.remote_filelist:
                            self.log.info(f"Starting to transfer from /{f} to {self.local_folderpath}/{f}")
                            sftp_client.get(f'/{os.path.join(self.remote_folderpath, f)}',
                                            f'{self.local_folderpath}/{f}')

                    else: #self.remote_filelist is None
                        regex = re.compile(f'.*{self.filetype}$')
                        filelist_with_filetype = [file_with_filetype for file_with_filetype in all_files if regex.match(file_with_filetype)]
                        regex = re.compile(self.remote_filename_pattern)
                        filelist_specific_pattern = [file_specific_pattern for file_specific_pattern in filelist_with_filetype
                                                     if regex.match(file_specific_pattern)]
                        filelist_specific_pattern=filelist_specific_pattern[:2]
                        for f in filelist_specific_pattern:
                            self.log.info(f"Starting to transfer from /{f} to {self.local_folderpath}/{f}")
                            sftp_client.get(f'/{os.path.join(self.remote_folderpath,f)}', f'{self.local_folderpath}/{f}')


                elif self.operation.lower() == SFTPOperation.GET: #upload
                    all_files = os.path.dirname(self.local_folderpath)
                    print(all_files)
                    self.log.info(f'Found {len(all_files)} files on server')
                    if self.local_filelist is not None:
                        for f in self.local_filelist:
                            self.log.info(f"Starting to transfer from /{f} to {self.remote_folderpath}/{f}")
                            sftp_client.put(f'{os.path.join(self.local_folderpath, f)}',f'/{os.path.join(self.remote_folderpath, f)}',)

                    else: #self.local_filelist is None: 구현필요
                        regex = re.compile(f'.*{self.filetype}$')
                        filelist_with_filetype = [file_with_filetype for file_with_filetype in all_files if regex.match(file_with_filetype)]
                        regex = re.compile(self.remote_filename_pattern)
                        filelist_specific_pattern = [file_specific_pattern for file_specific_pattern in filelist_with_filetype
                                                     if regex.match(file_specific_pattern)]
                        filelist_specific_pattern=filelist_specific_pattern[:2]
                        for f in filelist_specific_pattern:
                            self.log.info(f"Starting to transfer from /{f} to {self.local_folderpath}/{f}")
                            sftp_client.get(f'/{os.path.join(self.remote_folderpath, f)}',
                                            f'{self.local_folderpath}/{f}')



        except Exception as e:
            raise AirflowException(f"Error while transferring {file_msg}, error: {str(e)}")

        return (self.local_folderpath, self.remote_folderpath, self.operation)