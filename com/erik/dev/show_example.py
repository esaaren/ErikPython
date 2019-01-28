from datateam_helper_classes.etldb.ETLDatabaseHelper import ETLDatabaseHelper
from datateam_helper_classes.hiverunner.HiveRequestHelper import HiveRequestHelper


def run():
    parser = argparse.ArgumentParser()
    parser.add_argument('--url', help="The URL for the ETL control RDS", required=True)
    parser.add_argument('--username', help="The username for the ETL control RDS", required=True)
    parser.add_argument('--password', help="The password for the ETL control RDS", required=True)
    args = vars(parser.parse_args())

    db_url = args['url']
    db_username = args['username']
    db_password = args['password']

    control_db = ETLDatabaseHelper(datasource, db_url, db_username, db_password)
    hive_helper = HiveRequestHelper(datasource, control_db)

    #job = hive_helper.run_query(hdfs_qry)

    workflow = hivehelper.run_workflow('eriks_workflow')







    if job == 0:
        logger.info('it worked!')
    else:
        logger.info('it failed!')

    job2 = hive_helper.run_query(hdfs_qry)

    if job2 == 0:
        logger.info('it worked!')
    else:
        logger.info('it failed!')

    job3 = hive_helper.run_query(hdfs_qry)

    if job3 == 0:
        logger.info('it worked!')
    else:
        logger.info('it failed!')


if __name__ == '__main__':
    run()