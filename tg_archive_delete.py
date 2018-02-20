# this code archives the data older than one week and removes them.
# the original table is "transfer_exchangerate" and the table containing the old data is "transfer_exchangerate_archive"

import mysql.connector
from mysql.connector import errorcode
import time
import base64 # to encode the password, in python console: encoded = base64.b64encode('our password')

db_user = ''
db_password = ''
db_host = ''
db_database = ''


def archiveDeleteData(number_of_days):
    """
    This function gets the number of days we want to keep the data for and copies everything older than that in the archive
    table and deletes them from the main table to make free space available. It checks before carrying on with the archiving
    wether there is enough data (like more than 5,000 rows) already in the main table. Otherwise it doesnt proceed.
    :param number_of_days: number of days we want to go back.
    :return: returns nothing.
    """

    current_time = time.time()  # time is seconds from certain point in 1970
    last_week_time = current_time - number_of_days * 86400
    last_week_time = time.strftime('%Y-%m-%d %H:%M:%S',
                                   time.localtime(last_week_time))  # returns in a format readable in mysql

    try:
        conn = mysql.connector.connect(user=db_user, password=db_password,
                                  host=db_host, database=db_database)

        query_check_data = "select count(*) from tguruprod.transfer_exchangerate where created_date > %s"
        query_archive = "insert into tguruprod.transfer_exchangerate_archive select * from transfer_exchangerate where created_date < %s"
        query_delete = "delete from tguruprod.transfer_exchangerate where created_date < %s"
        query_count = "SELECT count(*) FROM tguruprod.transfer_exchangerate where created_date < %s" # was created before last week
        query_count_all = "SELECT count(*) FROM tguruprod.transfer_exchangerate_archive"

        cursor = conn.cursor()

        cursor.execute(query_check_data, (last_week_time,))
        num = cursor.fetchone()
        num_of_rows = num[0]

        if num_of_rows > 5000:

            cursor = conn.cursor()

            cursor.execute(query_count_all)
            for number in cursor:
                count_before = number[0]

            cursor.execute(query_count, (last_week_time,))
            for number in cursor:
                count_old_rows = number[0]

            cursor.execute(query_archive, (last_week_time,))
            cursor.execute(query_delete, (last_week_time,))

            cursor.execute(query_count_all)
            for number in cursor:
                count_after = number[0]

            if count_after == count_before + count_old_rows:
                conn.commit()
            else:
                print("there has been an issue with archiving")

        else:
            print 'not enough data to delete'

    except mysql.connector.Error as err:
        if err.errno == errorcode.ER_ACCESS_DENIED_ERROR:
            print("Something is wrong with your user name or password")
        elif err.errno == errorcode.ER_BAD_DB_ERROR:
            print("Database does not exist")
        else:
            print(err)
    else:
        cursor.close()
        conn.close()


archiveDeleteData(7)