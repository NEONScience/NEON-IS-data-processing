from contextlib import closing
from pandas import DataFrame
import datetime
from dateutil.relativedelta import relativedelta

from data_access.db_connector import DbConnector


def create_pub(connector: DbConnector, pub: DataFrame, version: str, change_by: str):
    connection = connector.get_connection()
    schema = connector.get_schema()
    domain_index = 1
    site_index = 2
    package_index = 11
    date_index = 10
    type_index = 6
    date_delimiter = "-"
    delimited_data_length = 14 # length of dot-delimited filename for data files. Used to find the package and data date
    timestamp = datetime.datetime.utcnow()

    dp_pub_sql = f'''
        INSERT INTO {schema}.dp_pub
           (dp_pub_id,
            dp_idq, 
            site, 
            package_type, 
            data_interval_start, 
            data_interval_end, 
            has_data, 
            status, 
            create_date, 
            update_date, 
            release_status,
            change_by)
        VALUES 
            (nextval('dp_pub_id_seq1'), %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        RETURNING dp_pub_id
    '''

    dp_pub_object_sql = f'''
        INSERT INTO {schema}.dp_pub_object
           (dp_pub_object_id,
            dp_pub_id,
            object_type,
            object_id,
            object_size,
            checksum,
            tran_date,
            supplier_version_id)
        VALUES 
            (nextval('dp_pub_object_id_seq1'), %s, %s, %s, %s, %s, %s, %s)
    '''

    find_dp_pub_sql = f'''
        select 
            dp_pub_id, release_status
        from 
            {schema}.dp_pub 
        where 
            dp_idq = %s and site = %s and data_interval_start = %s and data_interval_end = %s and package_type = %s
    '''

    delete_dp_pub_sql = f'''
        delete from {schema}.dp_pub where dp_pub_id = %s 
    '''

    has_data_by_package = {}
    objects_by_package = {}

    with closing(connection.cursor()) as cursor:
        try:
            # First run through the files in the manifest to find a data file
            for index, row in pub.iterrows():
                dp_parts = row['file'].split('.')
                # Parse package info from a data filename
                if len(dp_parts) == delimited_data_length:
                    # parse dp_pub fields common across the package
                    site = dp_parts[site_index]
                    dp_parts[domain_index] = 'DOM'
                    dp_parts[site_index] = 'SITE'
                    dp_idq = '.'.join(dp_parts[:6])
                    package_type = dp_parts[package_index]
                    date = dp_parts[date_index]
                    date_parts = date.split(date_delimiter)
                    year = int(date_parts[0])
                    month = int(date_parts[1])
                    data_interval_start = datetime.date(year, month, 1)
                    next_month = data_interval_start + relativedelta(days=+32)
                    data_interval_end = datetime.date(next_month.year, next_month.month, 1)
                    status = 'OK'
                    create_date = timestamp
                    update_date = timestamp
                    cursor.execute(find_dp_pub_sql, (dp_idq, site, data_interval_start, data_interval_end, package_type))
                    release_status = 'P'
                    existing_pubs = cursor.fetchall()
                    for existing_pub in existing_pubs:
                        if existing_pub[1] == 'T':
                            release_status = 'U'
                        else:
                            release_status = existing_pub[1]
                            cursor.execute(delete_dp_pub_sql, [existing_pub[0]])
                    break
                
            # Now run through all the files to create the pub record 
            for index, row in pub.iterrows():
                dp_parts = row['file'].split('.')
                has_data = 'Y' if row['hasData'] else 'N'
                
                data_type=dp_parts[type_index]
                object_type = 'DATA' # initialize
                if data_type == 'EML':
                    object_type = 'METADATA'
                elif data_type == 'readme':
                    object_type = 'README'
                elif data_type == 'sensor_positions':
                    object_type = 'SENSORLOCATIONS'
                elif data_type == 'variables':
                    object_type = 'VARIABLES'
                elif data_type == 'science_review_flags':
                    object_type = 'SCIENCEREVIEWFLAGS'
                
                object_id = row['objectId']
                object_size = row['size']
                checksum = row['checksum']
                tran_date = timestamp

                # update has_data
                if package_type in has_data_by_package.keys():
                    has_data_by_package[package_type] = 'Y' if (has_data_by_package[package_type] == 'Y'
                                                                or has_data == 'Y') else 'N'
                else:
                    has_data_by_package[package_type] = has_data
                    
                # store dp_pub_object tuple
                object_tuple = (object_type, object_id, object_size, checksum, tran_date, version)
                if package_type in objects_by_package.keys():
                    this_package = objects_by_package[package_type]
                    this_package.append(object_tuple)
                    objects_by_package[package_type] = this_package
                else:
                    objects_by_package[package_type] = [object_tuple]

            for package_type in objects_by_package.keys():
                # insert dp_pub and return ID
                cursor.execute(dp_pub_sql, (dp_idq, site, package_type, data_interval_start, data_interval_end,
                                            has_data_by_package[package_type], status, create_date, update_date,
                                            release_status, change_by))
                dp_pub_id = cursor.fetchone()[0]

                # insert dp_pub_objects
                for object_tuple in objects_by_package[package_type]:
                    cursor.execute(dp_pub_object_sql, (dp_pub_id,) + object_tuple)

            connection.commit()

        except Exception as exc:
            cursor.rollback()
            raise exc
