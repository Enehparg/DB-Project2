from datetime import datetime, timedelta

import sqlite3 as sqlite
import pymysql
import uuid
import json
import logging
from be.model import db_conn
from be.model import error


class Buyer(db_conn.DBConn):
    def __init__(self):
        db_conn.DBConn.__init__(self)

    def new_order(
        self, user_id: str, store_id: str, id_and_count: [(str, int)]
    ) -> (int, str, str):
        order_id = ""
        try:
            if not self.user_id_exist(user_id):
                return error.error_non_exist_user_id(user_id) + (order_id,)
            if not self.store_id_exist(store_id):
                return error.error_non_exist_store_id(store_id) + (order_id,)
            uid = "{}_{}_{}".format(user_id, store_id, str(uuid.uuid1()))

            for book_id, count in id_and_count:
                cursor = self.conn.execute(
                    "SELECT book_id, stock_level, book_info FROM store "
                    "WHERE store_id = %s AND book_id = %s;",
                    (store_id, book_id),
                )
                row = self.conn.fetchone()
                if row is None:
                    return error.error_non_exist_book_id(book_id) + (order_id,)

                stock_level = row[1]
                book_info = row[2]
                book_info_json = json.loads(book_info)
                price = book_info_json.get("price")

                if stock_level < count:
                    return error.error_stock_level_low(book_id) + (order_id,)

                cursor = self.conn.execute(
                    "UPDATE store set stock_level = stock_level - %s "
                    "WHERE store_id = %s and book_id = %s and stock_level >= %s; ",
                    (count, store_id, book_id, count),
                )
                if self.conn.rowcount == 0:
                    return error.error_stock_level_low(book_id) + (order_id,)

                self.conn.execute(
                    "INSERT INTO new_order_detail(order_id, book_id, count, price) "
                    "VALUES(%s, %s, %s, %s);",
                    (uid, book_id, count, price),
                )
                
            self.conn.execute(
                "INSERT INTO new_order(order_id, store_id, user_id, status) "
                "VALUES(%s, %s, %s, %s);",
                (uid, store_id, user_id, "待支付"),
            )

            self.conn.connection.commit()
            order_id = uid
        except pymysql.Error as e:
            print(e)
            logging.info("528, {}".format(str(e)))
            return 528, "{}".format(str(e)), ""
        except BaseException as e:
            logging.info("530, {}".format(str(e)))
            return 530, "{}".format(str(e)), ""

        return 200, "ok", order_id

    def payment(self, user_id: str, password: str, order_id: str) -> (int, str):
        conn = self.conn
        try:
            cursor = conn.execute(
                "SELECT order_id, user_id, store_id, status FROM new_order WHERE order_id = %s",
                (order_id,),
            )
            row = conn.fetchone()
            if row is None:
                return error.error_invalid_order_id(order_id)

            order_id = row[0]
            buyer_id = row[1]
            store_id = row[2]
            status = row[3]
            
            ###deliver部分，针对status的例外检测###

            if status != '待支付':
                return error.error_order_status(order_id)

            ###end###

            if buyer_id != user_id:
                return error.error_authorization_fail()

            cursor = conn.execute(
                'SELECT balance, password FROM user WHERE user_id = %s', (buyer_id,)
            )
            row = conn.fetchone()
            if row is None:
                return error.error_non_exist_user_id(buyer_id)
            balance = row[0]
            if password != row[1]:
                return error.error_authorization_fail()

            cursor = conn.execute(
                "SELECT store_id, user_id FROM user_store WHERE store_id = %s;",
                (store_id,),
            )
            row = conn.fetchone()
            if row is None:
                return error.error_non_exist_store_id(store_id)

            seller_id = row[1]

            if not self.user_id_exist(seller_id):
                return error.error_non_exist_user_id(seller_id)

            cursor = conn.execute(
                "SELECT book_id, count, price FROM new_order_detail WHERE order_id = %s;",
                (order_id,),
            )
            total_price = 0
            for row in conn:
                count = row[1]
                price = row[2]
                total_price = total_price + price * count

            if balance < total_price:
                return error.error_not_sufficient_funds(order_id)

            cursor = conn.execute(
                "UPDATE user set balance = balance - %s "
                "WHERE user_id = %s AND balance >= %s ",
                (total_price, buyer_id, total_price),
            )
            if conn.rowcount == 0:
                return error.error_not_sufficient_funds(order_id)

            cursor = conn.execute(
                'UPDATE user SET balance = balance + %s WHERE user_id = %s',
                (total_price, seller_id),
            )

            if conn.rowcount == 0:
                return error.error_non_exist_user_id(seller_id)

            cursor = conn.execute(
                "UPDATE new_order set status = %s WHERE order_id = %s",
                ('待发货', order_id),
            )

            #cursor = conn.execute(
            #    "DELETE FROM new_order WHERE order_id = %s", (order_id,)
            #)
            #if conn.rowcount == 0:
            #    return error.error_invalid_order_id(order_id)

            #cursor = conn.execute(
            #    "DELETE FROM new_order_detail where order_id = %s", (order_id,)
            #)
            #if conn.rowcount == 0:
            #    return error.error_invalid_order_id(order_id)

            conn.connection.commit()

        except pymysql.Error as e:
            print(e)
            return 528, "{}".format(str(e))

        except BaseException as e:
            return 530, "{}".format(str(e))

        return 200, "ok"

    def add_funds(self, user_id, password, add_value) -> (int, str):
        try:
            cursor = self.conn.execute(
                "SELECT password  from user where user_id=%s", (user_id,)
            )
            row = self.conn.fetchone()
            if row is None:
                return error.error_authorization_fail()

            if row[0] != password:
                return error.error_authorization_fail()

            cursor = self.conn.execute(
                "UPDATE user SET balance = balance + %s WHERE user_id = %s",
                (add_value, user_id),
            )
            if self.conn.rowcount == 0:
                return error.error_non_exist_user_id(user_id)

            self.conn.connection.commit()
        except pymysql.Error as e:
            return 528, "{}".format(str(e))
        except BaseException as e:
            return 530, "{}".format(str(e))

        return 200, "ok"

    def receive(self, user_id: str, password: str, order_id: str):
        try:
            self.conn.execute(
                "SELECT password  from user where user_id=%s", (user_id,)
            )
            row = self.conn.fetchone()
            if row is None:
                return error.error_authorization_fail()

            if row[0] != password:
                return error.error_authorization_fail()
            
            self.conn.execute(
                "SELECT order_id ,status FROM new_order WHERE order_id = %s",
                (order_id,),
            )

            if self.conn.rowcount == 0:
                return error.error_invalid_order_id(user_id)
            
            row = self.conn.fetchone()

            status = row[1]
            if status == "已完成":
                return error.error_order_status(order_id)
            if status == "待发货":
                return error.error_order_status(order_id)
            if status == "待支付":
                return error.error_order_status(order_id)
            
            self.conn.execute(
                "UPDATE new_order SET status = %s, complete_time=%s WHERE order_id = %s",
                ('已完成', datetime.now().strftime('%Y-%m-%d %H:%M:%S'), order_id),
            )

            self.conn.connection.commit()

        except pymysql.Error as e:
            print(e)
            return 528, "{}".format(str(e))
        except BaseException as e:
            return 530, "{}".format(str(e))
        return 200, "ok"
    
    def cancel(self, user_id:str, password : str, order_id: str):
        try:
            self.conn.execute(
                "SELECT password  from user where user_id=%s", (user_id,)
            )
            row = self.conn.fetchone()
            if row is None:
                return error.error_authorization_fail()

            if row[0] != password:
                return error.error_authorization_fail()
            
            self.conn.execute(
                "SELECT order_id,store_id,status FROM new_order WHERE order_id = %s",
                (order_id,),
            )

            row = self.conn.fetchone()
            if row is None:
                return error.error_invalid_order_id(order_id)
            
            store_id = row[1]
            status = row[2]
            if status != '待支付':
                return error.error_cancel(order_id)
            
            self.conn.execute(
                "SELECT book_id, count FROM new_order_detail WHERE order_id= %s",
                (order_id,),
            )
            row = self.conn.fetchall()
            for each in row:
                book_id = each[0]
                count = each[1]
                self.conn.execute(
                    "UPDATE store SET stock_level = stock_level + %s WHERE store_id=%s AND book_id=%s ",
                    (count, store_id, book_id),
                )

            self.conn.execute(
                "DELETE FROM new_order_detail WHERE order_id=%s",
                (order_id,),
            )

            self.conn.execute(
                "DELETE FROM new_order WHERE order_id=%s",
                (order_id,),
            )
            self.conn.connection.commit()
        except pymysql.Error as e:
            print(e)
            return 528, "{}".format(str(e))
        except BaseException as e:
            return 530, "{}".format(str(e))
        return 200, "ok"

    def history(self, user_id: str, password: str):
        try:
            self.conn.execute(
                "SELECT password  from user where user_id=%s", (user_id,)
            )
            row = self.conn.fetchone()
            if row is None:
                return error.error_authorization_fail()

            if row[0] != password:
                return error.error_authorization_fail()

            self.conn.execute(
                "SELECT order_id, status, complete_time from new_order WHERE user_id = %s",
                (user_id,),
            )

            row1 = self.conn.fetchall()
            orders = []
            order = {}
            books = []

            for each in row1:
                order_id = each[0]
                status = each[1]
                complete_time = each[2]

                self.conn.execute(
                    "SELECT book_id, count, price FROM new_order_detail WHERE order_id = %s",
                    (order_id,),
                )

                row2 = self.conn.fetchall()
                for book in row2:
                    a_book = [book[0],book[1],book[2]]
                    books.append(a_book)

                order = {
                    'order_id': order_id,
                    'status': status,
                    'completion_time': complete_time,
                    'books': books,
                }

                orders.append(order)

        except pymysql.Error as e:
            return 528, "{}".format(str(e)), ""
        except BaseException as e:
            return 530, "{}".format(str(e)), ""
        return 200, "ok", orders
    
    def search(self, store_id:str, title:str,tags: str,content: str):
        try:
            self.conn.execute(
                "SELECT store_id, book_id, book_info FROM store "
                "WHERE store_id LIKE %s "
                "AND JSON_EXTRACT(book_info, '$.title') LIKE %s "
                "AND JSON_EXTRACT(book_info, '$.tags') LIKE %s "
                "AND JSON_EXTRACT(book_info, '$.content') LIKE %s ",
                (store_id, title, tags, content)
            )

            books = []
            book = {}
            
            for row in self.conn.fetchall():
                a_store_id = row[0]
                book_id = row[1]
                info = json.loads(row[2])
                a_title = info['title']

                book = {
                    'store_id': a_store_id,
                    'book_id': book_id,
                    'title': a_title
                }

                books.append(book)

        except pymysql.Error as e:
            return 528, "{}".format(str(e)), ""
        except BaseException as e:
            return 530, "{}".format(str(e)), ""
        return 200, "ok", books