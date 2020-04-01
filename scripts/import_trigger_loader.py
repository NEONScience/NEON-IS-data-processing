from datetime import datetime, timedelta
import os


def dates_between(start_date, end_date):
    """
    Generate the dates between the start and end dates.
    :param start_date: Datetime to begin
    :param end_date: Datetime to end
    :return: List of dates.
    """
    delta = end_date - start_date  # as timedelta
    dates = []
    for i in range(delta.days + 1):
        date = start_date + timedelta(days=i)
        datetime_obj = datetime(date.year, date.month, date.day)
        year = datetime_obj.strftime('%Y')
        month = datetime_obj.strftime('%m')
        day = datetime_obj.strftime('%d')
        dates.append(f'/{year}/{month}/{day}')
    return dates


def get_sites():
    """
    Read in all sites from site file.
    :return: List of sites.
    """
    file_path = 'sites.txt'
    sites = []
    with open(file_path) as f:
        for line in f:
            line = line.strip()
            sites.append(line)
    return sites


def main():
    """
    Read all sites and generate dates.
    """
    command = 'pachctl'
    repo = 'import_trigger_wq@master'
    start_date = datetime.strptime('2020-01-01', '%Y-%m-%d')
    end_date = datetime.strptime('2020-01-05', '%Y-%m-%d')
    dates = dates_between(start_date, end_date)
    sites = get_sites()
    print('Starting commit.')
    os.system(f'{command} start commit {repo}')
    for date in dates:
        for site in sites:
            path = f'{date}/{site}'
            print(f'path: {path}')
            os.system(f'printf > filename | {command} put file -o {repo}:{path}')
    print('Finishing commit.')
    os.system(f'{command} finish commit {repo}')


if __name__ == '__main__':
    main()
