# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://docs.scrapy.org/en/latest/topics/item-pipeline.html
import json
from worldcat_scraper.databases import WorldcatScraperDatabase

class WorldcatScraperPipeline:
    def __init__(self):
        self.database = WorldcatScraperDatabase()

    def storeInDb(self, item):
        sql = "INSERT OR REPLACE INTO books ({0}) VALUES ({1});".format(','.join(item.keys()), ','.join(['?'] * len(item.keys())))

        values = []
        for x in item.values():
            # need to convert dicts to JSON string
            if type(x) is dict:
                values.append(json.dumps(x))
            else:
                values.append(x)
        self.database.dbExecute(sql, tuple(values))

    def process_item(self, item, spider):
        self.storeInDb(item)
        return item
