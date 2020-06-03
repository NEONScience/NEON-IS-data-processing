import argparse
import python_pachyderm
from pathlib import Path

import scale_testing.job_data_finder as data_finder
from dag.dag_manager import DagManager


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--host', required=True, help='Only the hostname of a grpc URL.')
    parser.add_argument('--port', required=True, help='The port number.')
    parser.add_argument('--specification', required=True, help='A DAG end node pipeline specification path.')
    parser.add_argument('--specifications', required=True,
                        help='A path containing pipeline specification files.')
    args = parser.parse_args()
    host = args.host
    port = int(args.port)
    specification = args.specification
    specifications = args.specifications

    print(f'host: {host}')
    print(f'port: {port}')
    print(f'specification: {specification}')
    print(f'specifications: {specifications}')

    client = python_pachyderm.Client(host=host, port=port)

    dag_manager = DagManager(Path(specification), Path(specifications))
    dag_builder = dag_manager.get_dag_builder()
    pipeline_names = dag_builder.get_pipeline_names()

    for pipeline_name in pipeline_names:
        job = data_finder.get_most_recent_job_info(client, pipeline_name)
        job_data = data_finder.get_job_run_data(job)
        upload_time = job_data.get('upload_time')
        download_time = job_data.get('download_time')
        process_time = job_data.get('process_time')
        print(f'up time: {upload_time} down time: {download_time} p time {process_time}')


if __name__ == '__main__':
    main()
