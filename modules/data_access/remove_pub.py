from contextlib import closing
import datetime
from structlog import get_logger
from data_access.db_connector import DbConnector
log = get_logger()


def remove_pub(connector: DbConnector, pub_records: dict, change_by:str):
    """
    Remove publication packages from LATEST visibility. 
    Any pub records for the package with release_status not equal to ‘T' (tagged) will be deleted, which will 
    likely be any release_status = 'U' (updated) or 'P’ (provisional) records.
    If there are any release_status = 'T' records present for the package (existing release), 
    a new pub record will be written for the package with release_status = ‘U' and status = 'NODATA’

    :param connector: A database connector.
    :param pub_records: A dictionary list of publication packages. Each dictionary key refers to a 
        product-site-month-package. The name of the key is arbitrary, but the dictionary contents for 
        each key must be a list of the existing publication records for the product-site-month-package as DpPub type
    :param change_by: The user to specify in the change_by field for any inserted records
    :return: Nothing except for records removed and/or inserted into the database, as described above.
    """

    connection = connector.get_connection()
    schema = connector.get_schema()
    timestamp = datetime.datetime.utcnow()
    hasData = 'N'
    status='NODATA'

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
    
    delete_dp_pub_sql = f'''
        delete from {schema}.dp_pub where dp_pub_id = %s 
    '''

    with closing(connection.cursor()) as cursor:
        try:
            # Go through each set of pub records to check/adjust accessiblity
            # Each pub key is a product-site-month-package. Contents are a list of associated DpPub records
            # Delete any pub records with release_status != ‘T' (tagged). This will likely be any status = 'U' (updated) or 'P’ (provisional).
            # Write a new pub record with release_status = 'U' if there are release-tagged records or 'P' if there are no release-tagger records and status = 'NODATA’
            for pub_key in pub_records.keys():

                release_status = 'P' # provisional
                # Remove any non-tagged pub records
                for pub_record in pub_records[pub_key]:
                    
                    if pub_record.releaseStatus == 'T':
                        release_status = 'U' # updated
                        log.debug(f'Retaining release-tagged pub record: {pub_record}')
                    else:
                        # Delete the pub record
                        log.info(f'Deleting pub record: {pub_record}')
                        cursor.execute(delete_dp_pub_sql, [pub_record.id])
                        
                    
                # Create 'updated' or 'provisional' record, as appropriate, with status=NODATA
                # Take the common pub fields from the last pub record for this key
                pub_record_new = (pub_record.dataProductId, 
                                  pub_record.site, 
                                  pub_record.packageType, 
                                  pub_record.dataIntervalStart, 
                                  pub_record.dataIntervalEnd,
                                  hasData,
                                  status,
                                  timestamp, 
                                  timestamp,
                                  release_status,
                                  change_by)
                log.debug(f'Creating new pub record: {pub_record_new}')
                dp_pub_id = cursor.execute(dp_pub_sql, pub_record_new)

            connection.commit()

        except Exception as exc:
            cursor.rollback()
            raise exc
