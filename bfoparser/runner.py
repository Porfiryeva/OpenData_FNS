from scrapy.crawler import CrawlerProcess
from scrapy.settings import Settings

from bfoparser import settings
from bfoparser.spiders.bonalogru import BonalogruSpider

import pickle
from sys import argv

# для теста
# inns = ['7731334562',
#          '7731334770',
#          '7731345677',
#          '7731346938',
#          '7731322486',
#          '7731323899',
#          '7731334756',
#          '7731339747',
#          '7731325134',
#          '7731331762',
#          '7731320231',
#          '7731345437',
#          '7731341640',
#          '7731321796',
#          '7731332452',
#          '7731346487',
#          '7731322359',
#          '7731332903',
#          '7731339761']
# пока так напрямую
inn_path = r'D:/docs/GB/Финальный проект/parsed_data/inn/inn_6/inn_65.pickle'
with open(inn_path, 'rb') as f:
    inns = pickle.load(f)

if __name__ == '__main__':
    # # чтобы скормить пауку порцию инн
    # inn_path = argv[1]
    # # список строк
    # with open(inn_path, 'rb') as f:
    #     inns = pickle.load(f)

    crawler_settings = Settings()
    crawler_settings.setmodule(settings)

    process = CrawlerProcess(settings=crawler_settings)
    process.crawl(BonalogruSpider, inns=inns, year='2021')

    process.start()
