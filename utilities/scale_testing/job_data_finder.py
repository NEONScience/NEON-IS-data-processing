import argparse

import python_pachyderm
from python_pachyderm import Client
from python_pachyderm.proto.pps.pps_pb2 import JobInfo


def get_latest_job(client: Client, pipeline_name: str) -> JobInfo:
    max_milliseconds = 0
    latest_job = None
    for job_info in client.list_job(pipeline_name=pipeline_name, history=0, full=False):
        state = job_info.state
        if state == 3:  # 3 means the job is complete
            started_milliseconds = job_info.started.seconds
            if started_milliseconds > max_milliseconds:
                max_milliseconds = started_milliseconds
                latest_job = job_info
    return latest_job


def get_job_run_times(job_info: JobInfo) -> dict:
    datums_processed = job_info.data_processed
    job_stats = job_info.stats
    if job_stats is not None:
        started = job_info.started
        started_milliseconds = started.seconds
        started_nanos = started.nanos
        finished = job_info.finished
        finished_milliseconds = finished.seconds
        finished_nanos = finished.nanos
        download_time = job_stats.download_time
        download_seconds = download_time.seconds
        download_nanos = download_time.nanos
        process_time = job_stats.process_time
        process_seconds = process_time.seconds
        process_nanos = process_time.nanos
        upload_time = job_stats.upload_time
        upload_seconds = upload_time.seconds
        upload_nanos = upload_time.nanos
        times = {
            'started': started_milliseconds,
            'started_nanos': started_nanos,
            'finished': finished_milliseconds,
            'finished_nanos': finished_nanos,
            'download': download_seconds,
            'download_nanos': download_nanos,
            'upload': upload_seconds,
            'upload_nanos': upload_nanos,
            'process': process_seconds,
            'process_nanos': process_nanos,
            'datums_processed': datums_processed
        }
        return times


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument('--host', required=True,
                        help='Only the hostname part of a grpc URL.')
    parser.add_argument('--port', required=True, help='The port number.')
    parser.add_argument('--pipeline', required=True, help='A pipeline name.')
    args = parser.parse_args()
    host = args.host
    port = int(args.port)
    pipeline_name = args.pipeline
    client = python_pachyderm.Client(host=host, port=port)
    job = get_latest_job(client, pipeline_name)
    if job is None:
        print(f'No jobs are available for {pipeline_name}.')
    else:
        get_job_run_times(job)


if __name__ == '__main__':
    main()
