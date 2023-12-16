import logging
import os
import sqlite3 as sqlite
import pymysql


class Store:
    database: str

    def __init__(self, db_path):
        #self.database = os.path.join(db_path, "be.db")
        self.init_tables()

    def init_tables(self):
        try:
            conn = self.get_db_conn()
            conn.execute("""
                CREATE TABLE IF NOT EXISTS user(
                    user_id VARCHAR(150) PRIMARY KEY,
                    password VARCHAR(150) NOT NULL,
                    balance INT NOT NULL,
                    token TEXT,
                    terminal TEXT
                )
            """)

            conn.execute("""
                CREATE TABLE IF NOT EXISTS user_store(
                    user_id VARCHAR(150),
                    store_id VARCHAR(150),
                    primary key(user_id, store_id)
                )
            """)

            conn.execute("""
                CREATE TABLE IF NOT EXISTS store(
                    store_id VARCHAR(150),
                    book_id VARCHAR(150),
                    book_info LONGTEXT,
                    stock_level INT,     
                    primary key(store_id, book_id)
                )
            """)

            conn.execute("""
                CREATE TABLE IF NOT EXISTS new_order(
                    order_id VARCHAR(200) primary key,
                    user_id VARCHAR(100),
                    store_id VARCHAR(100),
                    status VARCHAR(20)
                )
            """)

            conn.execute("""
                CREATE TABLE IF NOT EXISTS new_order_detail(
                    order_id VARCHAR(200),
                    book_id VARCHAR(100),
                    count INT,
                    price INT,
                    primary key(order_id, book_id)
                )
            """)

            conn.connection.commit()
        except pymysql.Error as e:
            logging.error(e)
            conn.connection.rollback()

    def get_db_conn(self) :
        return pymysql.connect(host = 'localhost', user='root', passwd='root', port=3306, database='bookstore').cursor()


database_instance: Store = None


def init_database(db_path):
    global database_instance
    database_instance = Store(db_path)


def get_db_conn():
    global database_instance
    return database_instance.get_db_conn()
