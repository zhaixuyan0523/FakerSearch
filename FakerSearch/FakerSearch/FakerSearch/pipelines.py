# -*- coding: utf-8 -*-

# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://doc.scrapy.org/en/latest/topics/item-pipeline.html
from scrapy.pipelines.images import ImagesPipeline
from scrapy.exporters import JsonItemExporter
import codecs
import json
import pymysql
from w3lib.html import remove_tags
from FakerSearch.models.es_types import ArticleType
from twisted.enterprise import adbapi


class FakersearchPipeline(object):
    def process_item(self, item, spider):
        return item


# 图片处理
class ArticleImagePipeline(ImagesPipeline):
    def item_completed(self, results, item, info):
        if "front_image_url" in item:
            for ok, value in results:
                image_file_path = value['path']
            item['front_image_path'] = image_file_path
        return item


# class JsonWithEncodingPipeline(object):
#     # 自定义json文件的导出
#     def __init__(self):
#         self.file = codecs.open('article.json', 'w', encoding="utf-8")
#
#     def process_item(self, item, spider):
#         # 将item转换为dict，然后生成json对象，false避免中文出错
#         lines = json.dumps(dict(item), ensure_ascii=False) + "\n"
#         self.file.write(lines)
#         return item
#     # 当spider关闭的时候
#
#     def spider_closed(self, spider):
#         self.file.close()
#
#
# class JsonExporterPipleline(object):
#     # 调用scrapy提供的json export导出json文件
#     def __init__(self):
#         self.file = open('articleexport.json', 'wb')
#         self.exporter = JsonItemExporter(self.file, encoding="utf-8", ensure_ascii=False)
#         self.exporter.start_exporting()
#
#     def  close_spider(self, spider):
#         self.exporter.finish_exporting()
#         self.file.close()
#
#     def process_item(self, item, spider):
#         self.exporter.export_item(item)
#         return item


# class MysqlPipeline(object):
#     def __init__(self):
#         self.conn = pymysql.connect('localhost', 'root', '19970523zxy', 'faker_search', charset='utf8', use_unicode=True)
#         self.cursor = self.conn.cursor()
#
#     def process_item(self, item, spider):
#         insert_sql = """
#             insert into jobbole_article(title,create_date,url,url_object_id,front_image_url,front_image_path,praise_nums,comment_nums,fav_nums,tags,content)
#             values(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
#         """
#         self.cursor.execute(insert_sql, (
#             item['title'], item['create_date'], item['url'], item['url_object_id'],
#             item['front_image_url'], item['front_image_path'], item['praise_nums'],
#             item['comment_nums'], item['fav_nums'], item['tags'], item['content'])
#         )
#         self.conn.commit()


class MysqlTwistedPipeline(object):
    def __init__(self, dbpool):
        self.dbpool = dbpool

    @classmethod
    def from_settings(cls, settings):
        db_params = dict(
            host=settings["MYSQL_HOST"],
            db=settings["MYSQL_DBNAME"],
            user=settings["MYSQL_USER"],
            passwd=settings["MYSQL_PASSWORD"],
            charset='utf8',
            cursorclass=pymysql.cursors.DictCursor,
            use_unicode=True,
        )
        # Twister 只是提供了一个异步的容器，并没有提供数据库连接，所以连接数据库还是要用 pymysql 进行连接
        # adbapi 可以将 MySQL 的操作变为异步
        # ConnectionPool 第一个参数是我们连接数据库所使用的 库名，这里是连接 MySQL 用的 pymysql
        # 第二个参数就是 pymysql 连接操作数据库所需的参数，这里将参数组装成字典 db_params，当作关键字参数传递进去

        dbpool = adbapi.ConnectionPool('pymysql', **db_params)
        return cls(dbpool)

    def process_item(self, item, spider):
        # 使用 Twisted 将 MYSQL 插入变成异步
        # 执行 runInteraction 方法的时候会返回一个 query 对象，专门用来处理异常
        query = self.dbpool.runInteraction(self.do_insert, item)
        query.addErrback(self.handle_error, item, spider)

    def do_insert(self, cursor, item):
        # 执行具体的插入
        # 根据不同的item 构建不同的sql语句并插入到mysql中
        insert_sql, params = item.get_insert_sql()
        cursor.execute(insert_sql, params)

    def handle_error(self, failure, item, spider):
        # 异常处理方法，处理异步插入数据库时产生的异常
        # failure 参数不需要我们自己传递，出现异常会自动将异常当作这个参数传递进来
        print(f'出现异常：{failure}')





#####################################es相关########################################################

# class ElasticsearchPipeline(object):
#     # 将数据写入到es当中
#     def process_item(self, item, spider):
#
#         article = ArticleType()
#         article.title = item['title']
#         article.create_date = item['create_date']
#         article.content = remove_tags(item['content'])
#         article.front_image_url = item['front_image_url']
#         if 'front_image_path' in item:
#             article.front_image_path = item['front_image_path']
#         article.praise_nums = item['praise_nums']
#         article.comment_nums = item['comment_nums']
#         article.fav_nums = item['fav_nums']
#         article.url = item['url']
#         article.tags = item['tags']
#         article.meta.id = item['url_object_id']
#         article.save()
#         return item

class ElasticsearchPipeline(object):
    # 将数据写入到es当中
    def process_item(self, item, spider):
        item.save_to_es()
        return item