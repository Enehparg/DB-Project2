import pytest

from fe.access.seller import Seller
from fe.access.buyer import Buyer
from fe.test.gen_book_data import GenBook
from fe.access.new_buyer import register_new_buyer
from fe.access.book import Book
import uuid


class TestSearch:

    @pytest.fixture(autouse=True)
    def pre_run_initialization(self):
        self.seller_id = "test_new_order_seller_id_{}".format(str(uuid.uuid1()))
        self.store_id = "test_new_order_store_id_{}".format(str(uuid.uuid1()))
        self.buyer_id = "test_new_order_buyer_id_{}".format(str(uuid.uuid1()))
        self.password = self.seller_id
        gen_book = GenBook(self.seller_id, self.store_id)
        self.seller = gen_book.seller

        ok, buy_book_id_list = gen_book.gen(
            non_exist_book_id=False, low_stock_level=False, max_book_count=5
        )
        self.buy_book_info_list = gen_book.buy_book_info_list
        assert ok
        b = register_new_buyer(self.buyer_id, self.password)
        self.buyer = b
        
        yield

    def test_ok(self):
        code = self.buyer.search(self.store_id,"","","")
        assert code == 200