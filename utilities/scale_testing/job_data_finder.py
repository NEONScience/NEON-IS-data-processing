import argparse

import python_pachyderm
from python_pachyderm import Client
from python_pachyderm.proto.pps.pps_pb2 import JobInfo


def get_most_recent_job_info(client: Client, pipeline_name: str):
    max_milliseconds = 0
    most_recent_job = None
    for job_info in client.list_job(pipeline_name=pipeline_name, history=0, full=False):
        state = job_info.state
        if state == 3:  # 3 means the job is complete
            started_milliseconds = job_info.started.seconds
            if started_milliseconds > max_milliseconds:
                max_milliseconds = started_milliseconds
                most_recent_job = job_info
    return most_recent_job


def get_job_run_data(job_info: JobInfo):
    started_milliseconds = job_info.started.seconds
    started_nanos = job_info.started.nanos
    print(f'started_millis: {started_milliseconds}.{started_nanos}')
    finished_milliseconds = job_info.finished.seconds
    finished_nanos = job_info.finished.nanos
    print(f'finished_millis: {finished_milliseconds}.{finished_nanos}')
    job_stats = job_info.stats
    if job_stats is not None:
        download_time = job_stats.download_time
        download_seconds = download_time.seconds
        download_nanos = download_time.nanos
        process_time = job_stats.process_time
        process_seconds = process_time.seconds
        process_nanos = process_time.nanos
        upload_time = job_stats.upload_time
        upload_seconds = upload_time.seconds
        upload_nanos = upload_time.nanos
        print(f'download: {download_seconds}.{download_nanos} \
            upload: {upload_seconds}.{upload_nanos} \
            process: {process_seconds}.{process_nanos}')
        times = {'download': download_seconds, 'upload': upload_seconds, 'process': process_seconds}
        return times


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--host', required=True, help='Only the hostname of a grpc URL.')
    parser.add_argument('--port', required=True, help='The port number.')
    parser.add_argument('--pipeline', required=True, help='A pipeline name.')
    args = parser.parse_args()
    host = args.host
    port = int(args.port)
    pipeline_name = args.pipeline
    client = python_pachyderm.Client(host=host, port=port)
    job = get_most_recent_job_info(client, pipeline_name)
    get_job_run_data(job)


if __name__ == '__main__':
    main()
