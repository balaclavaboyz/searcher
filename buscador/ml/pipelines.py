# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://docs.scrapy.org/en/latest/topics/item-pipeline.html


# useful for handling different item types with a single interface
# from itemadapter import ItemAdapter
import sqlite3

class MlPipeline:
    def __init__(self):
        self.con=sqlite3.connect('tmp.db')
        self.cur=self.con.cursor()
        self.create_table()

    def create_table(self):
        self.cur.execute("""
            DROP TABLE IF EXISTS products
        """)
        self.cur.execute("""CREATE TABLE products(
            title text,
            price real,
            link text,
            unit text
        )""")

    def process_item(self, item, spider):
        self.cur.execute("""insert or ignore into products VALUES(?,?,?,?)""",(
            item['title'],item['price'],item['link'],item['unit']
        ))
        self.con.commit()
        return item