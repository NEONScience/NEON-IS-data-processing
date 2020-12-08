import argparse
import python_pachyderm
from pathlib import Path

import scale_testing.job_data_finder as data_finder
from dag.pipeline_specification_parser import PipelineSpecificationParser
from dag.dag_manager import DagManager


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument('--host', required=True,
                        help='Only the hostname of a grpc URL.')
    parser.add_argument('--port', required=True, help='The port number.')
    parser.add_argument('--specification', required=True,
                        help='A DAG end node pipeline specification path.')
    parser.add_argument('--specifications', required=True,
                        help='A path containing pipeline specification files.')
    args = parser.parse_args()
    host = args.host
    port = int(args.port)
    specification = Path(args.specification)
    specifications = Path(args.specifications)

    print(f'host: {host}')
    print(f'port: {port}')
    print(f'specification: {specification}')
    print(f'specifications: {specifications}')

    client = python_pachyderm.Client(host=host, port=port)

    parser = PipelineSpecificationParser(specification, specifications)
    dag_manager = DagManager(parser)
    dag_builder = dag_manager.get_dag_builder()
    pipeline_names = dag_builder.get_pipeline_names()

    total_upload = 0
    total_download = 0
    total_process = 0
    for pipeline_name in pipeline_names:
        job = data_finder.get_latest_job_stats(client, pipeline_name)
        if job is None:
            print(f'No jobs are available for {pipeline_name}')
        else:
            job_data = data_finder.get_job_run_times(job)
            upload_time = job_data.get('upload')
            download_time = job_data.get('download')
            process_time = job_data.get('process')
            print(f'pipeline: {pipeline_name} '
                  f'upload time: {upload_time} sec. '
                  f'download time: {download_time} sec. '
                  f'process time {process_time} sec.')
            if upload_time is not None:
                total_upload += upload_time
            if download_time is not None:
                total_download += download_time
            if process_time is not None:
                total_process += process_time
    print(f'total upload: {total_upload} sec. '
          f'total download: {total_download} sec. '
          f'total_process: {total_process} sec.')


if __name__ == '__main__':
    main()
