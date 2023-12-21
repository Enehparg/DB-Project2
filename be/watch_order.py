import pymysql
import schedule
import time
from datetime import datetime

def auto_delete():
    db = pymysql.connect(host='localhost', user='root', passwd='root', port=3306, database='bookstore1')
    cursor = db.cursor()

    cursor.execute(
        "SELECT order_id, store_id FROM new_order WHERE TTL IS NOT NULL AND TTL < %s",
        (datetime.now(),),
    )

    rows = cursor.fetchall()

    for each in rows:
        order_id = each[0]
        store_id = each[1]

        cursor.execute(
            "SELECT book_id, count FROM new_order_detail WHERE order_id = %s",
            (order_id,),
        )

        for row in cursor.fetchall():
            book_id = row[0]
            count = row[1]
            cursor.execute(
                    "UPDATE store SET stock_level = stock_level + %s WHERE store_id=%s AND book_id=%s ",
                    (count, store_id, book_id),
                )
        
        cursor.execute(
                "DELETE FROM new_order_detail WHERE order_id=%s",
                (order_id,),
            )

        cursor.execute(
                "DELETE FROM new_order WHERE order_id=%s",
                (order_id,),
            )
    cursor.connection.commit()

    cursor.close()
    db.close()

schedule.every().hour.do(auto_delete)

while True:
    schedule.run_pending()
    time.sleep(1)
