import scrapy
from scrapy import Request
from scrapy.http import HtmlResponse
from bfoparser.items import BfoparserItem
from copy import deepcopy


class BonalogruSpider(scrapy.Spider):
    name = "bonalogru"
    allowed_domains = ["bo.nalog.ru"]

    # start_urls = ["http://bo.nalog.ru/"]

    # чтобы передать конструктору инн и year
    def __init__(self, **kwargs):
        super().__init__()
        self.inns = kwargs.get("inns")
        self.year = kwargs.get("year")

    # далее - переопределить - унаследовать? дополнить?
    # будет возвращать yeld в цикле для inn
    def start_requests(self):
        # на самом деле inns надо было дополнительно почистить до этого!
        for inn in self.inns:
            start_url = f'https://bo.nalog.ru/nbo/organizations/search?query={inn}&page=0'
            yield Request(start_url, dont_filter=True)

    # дальше - чтобы не было гонки - передавая deepcopy - около 3 функций
    def parse(self, response: HtmlResponse):
        # отсюда - только id карточки организации, если не пустая
        data = response.json()
        if data['content']:
            # мб здесь - проверка для возвращаемой структуры?
            id_org = data['content'][0]['id']
            # следующий переход
            url_org_card = f'https://bo.nalog.ru/nbo/organizations/{id_org}'
            yield response.follow(url_org_card, callback=self.parse_org_card)

    def parse_org_card(self, response):
        data = response.json()
        org_info = {}
        # явно перечисляем ключи, так как проще видеть, что забираем
        # и легче выключить/включить какие-то данные
        # в качестве _id - инн, так как по нему стыкуем с xml
        org_info['_id'] = data['inn']  # и здесь можно было почистить inn
        org_info['id_org'] = data['id']
        org_info['shortName'] = data['shortName']
        org_info['ogrn'] = data['ogrn']
        org_info['index'] = data['index']
        org_info['region'] = data['region']
        org_info['district'] = data['district']
        org_info['city'] = data['city']
        org_info['settlement'] = data['settlement']
        org_info['okved'] = data['okved']  # видимо, старая версия?
        # полагаю, что у некоторых okvd2 будет Null, а okvd - словарь, по
        # аналогии с okopf, ага, есть такие, у которых оба null
        if data['okved2']:
            org_info['okved2'] = data.get('okved2').get('id')  # общерос классиф видов эк. деят-ти
        else:
            org_info['okved2'] = data.get('okved2')
        # у некоторых okopf=Null, а не словарь
        # не факт, что мне нужны такие итемы
        if data['okopf']:
            org_info['okopf'] = data['okopf']['id']  # см справочник okopf.csv
        else:
            org_info['okopf'] = data['okopf']
        org_info['statusCode'] = data['statusCode']
        # здесь для уверенности стоило собирать statusDate - дата статуса
        org_info['registrationDate'] = data['registrationDate']
        org_info['authorizedCapital'] = data['authorizedCapital']

        url_report_id = f'https://bo.nalog.ru/nbo/organizations/{org_info["id_org"]}/bfo/'
        yield response.follow(url_report_id, callback=self.parse_report_id,
                              cb_kwargs={'org_info': deepcopy(org_info)})

    def parse_report_id(self, response, org_info):
        # здесь собираем небольшой словарь период отчёта: id отчёта
        data = response.json()
        report_periods = {reports_card['period']: reports_card['id'] for reports_card in data}
        if self.year in report_periods:
            id_period = report_periods[self.year]
            org_info['report_periods'] = report_periods

            url_reports = f'https://bo.nalog.ru/nbo/bfo/{id_period}/details'
            yield response.follow(url_reports, callback=self.parse_reports,
                                  cb_kwargs={'org_info': deepcopy(org_info)})

    def parse_reports(self, response, org_info):
        data = response.json()[0]
        balance = data['balance']
        fin_res = data['financialResult']
        # далее - опять явно указываю столбцы, чтобы контролировать, что забираю - и включать/выключать
        # их много
        # смешиваю balance и fin_result -  так как коды не пересекаются
        if (balance['okud'] == '0710001') and (fin_res['okud'] == '0710002'):
            # АКТИВ
            # I Внеоборотные активы
            # Нематериальные активы
            org_info[f'{self.year}_1110'] = balance.get('current1110')
            org_info[f'{int(self.year) - 1}_1110'] = balance.get('previous1110')
            org_info[f'{int(self.year) - 2}_1110'] = balance.get('beforePrevious1110')
            # Результаты исследований и разработок
            # 1120 здесь некорректно - их не беру!
            org_info[f'{self.year}_1120'] = balance.get('current1120')
            org_info[f'{int(self.year) - 1}_1120'] = balance.get('previous1120')
            org_info[f'{int(self.year) - 2}_1120'] = balance.get('beforePrevious1120')
            # Нематериальные поисковые активы
            org_info[f'{self.year}_1130'] = balance.get('current1130')
            org_info[f'{int(self.year) - 1}_1130'] = balance.get('previous1130')
            org_info[f'{int(self.year) - 2}_1130'] = balance.get('beforePrevious1130')
            # Материальные поисковые активы
            org_info[f'{self.year}_1140'] = balance.get('current1140')
            org_info[f'{int(self.year) - 1}_1140'] = balance.get('previous1140')
            org_info[f'{int(self.year) - 2}_1140'] = balance.get('beforePrevious1140')
            # Основные средства
            org_info[f'{self.year}_1150'] = balance.get('current1150')
            org_info[f'{int(self.year) - 1}_1150'] = balance.get('previous1150')
            org_info[f'{int(self.year) - 2}_1150'] = balance.get('beforePrevious1150')
            # Доходные вложения в материальные ценности
            org_info[f'{self.year}_1160'] = balance.get('current1160')
            org_info[f'{int(self.year) - 1}_1160'] = balance.get('previous1160')
            org_info[f'{int(self.year) - 2}_1160'] = balance.get('beforePrevious1160')
            # Финансовые вложения
            org_info[f'{self.year}_1170'] = balance.get('current1170')
            org_info[f'{int(self.year) - 1}_1170'] = balance.get('previous1170')
            org_info[f'{int(self.year) - 2}_1170'] = balance.get('beforePrevious1170')
            # Отложенные налоговые активы
            org_info[f'{self.year}_1180'] = balance.get('current1180')
            org_info[f'{int(self.year) - 1}_1180'] = balance.get('previous1180')
            org_info[f'{int(self.year) - 2}_1180'] = balance.get('beforePrevious1180')
            # Прочие внеоборотные активы
            org_info[f'{self.year}_1190'] = balance.get('current1190')
            org_info[f'{int(self.year) - 1}_1190'] = balance.get('previous1190')
            org_info[f'{int(self.year) - 2}_1190'] = balance.get('beforePrevious1190')
            # Итого по разделу I
            org_info[f'{self.year}_1100'] = balance.get('current1100')
            org_info[f'{int(self.year) - 1}_1100'] = balance.get('previous1100')
            org_info[f'{int(self.year) - 2}_1100'] = balance.get('beforePrevious1100')
            # II Оборотные активы
            # Запасы
            org_info[f'{self.year}_1210'] = balance.get('current1210')
            org_info[f'{int(self.year) - 1}_1210'] = balance.get('previous1210')
            org_info[f'{int(self.year) - 2}_1210'] = balance.get('beforePrevious1210')
            # Налог на добавленную стоимость по приобретенным ценностям
            # ага здесь ошибка - 1220 записываю в 1120
            org_info[f'{self.year}_1220'] = balance.get('current1220')
            org_info[f'{int(self.year) - 1}_1220'] = balance.get('previous1220')
            org_info[f'{int(self.year) - 2}_1220'] = balance.get('beforePrevious1220')
            # Дебиторская задолженность
            org_info[f'{self.year}_1230'] = balance.get('current1230')
            org_info[f'{int(self.year) - 1}_1230'] = balance.get('previous1230')
            org_info[f'{int(self.year) - 2}_1230'] = balance.get('beforePrevious1230')
            # Финансовые вложения (за исключением денежных эквивалентов)
            org_info[f'{self.year}_1240'] = balance.get('current1240')
            org_info[f'{int(self.year) - 1}_1240'] = balance.get('previous1240')
            org_info[f'{int(self.year) - 2}_1240'] = balance.get('beforePrevious1240')
            # Денежные средства и денежные эквиваленты
            org_info[f'{self.year}_1250'] = balance.get('current1250')
            org_info[f'{int(self.year) - 1}_1250'] = balance.get('previous1250')
            org_info[f'{int(self.year) - 2}_1250'] = balance.get('beforePrevious1250')
            # Прочие оборотные активы
            org_info[f'{self.year}_1260'] = balance.get('current1260')
            org_info[f'{int(self.year) - 1}_1260'] = balance.get('previous1260')
            org_info[f'{int(self.year) - 2}_1260'] = balance.get('beforePrevious1260')
            # Итого по разделу II
            org_info[f'{self.year}_1200'] = balance.get('current1200')
            org_info[f'{int(self.year) - 1}_1200'] = balance.get('previous1200')
            org_info[f'{int(self.year) - 2}_1200'] = balance.get('beforePrevious1200')
            # Баланс по АКТИВЫ
            org_info[f'{self.year}_1600'] = balance.get('current1600')
            org_info[f'{int(self.year) - 1}_1600'] = balance.get('previous1600')
            org_info[f'{int(self.year) - 2}_1600'] = balance.get('beforePrevious1600')
            # ПАССИВ
            # III Капитал и резервы
            # Уставный капитал (складочный капитал, уставный фонд, вклады товарищей)
            org_info[f'{self.year}_1310'] = balance.get('current1310')
            org_info[f'{int(self.year) - 1}_1310'] = balance.get('previous1310')
            org_info[f'{int(self.year) - 2}_1310'] = balance.get('beforePrevious1310')
            # Собственные акции, выкупленные у акционеров
            org_info[f'{self.year}_1320'] = balance.get('current1320')
            org_info[f'{int(self.year) - 1}_1320'] = balance.get('previous1320')
            org_info[f'{int(self.year) - 2}_1320'] = balance.get('beforePrevious1320')
            # Переоценка внеоборотных активов
            org_info[f'{self.year}_1340'] = balance.get('current1340')
            org_info[f'{int(self.year) - 1}_1340'] = balance.get('previous1340')
            org_info[f'{int(self.year) - 2}_1340'] = balance.get('beforePrevious1340')
            # Добавочный капитал (без переоценки)
            org_info[f'{self.year}_1350'] = balance.get('current1350')
            org_info[f'{int(self.year) - 1}_1350'] = balance.get('previous1350')
            org_info[f'{int(self.year) - 2}_1350'] = balance.get('beforePrevious1350')
            # Резервный капитал
            org_info[f'{self.year}_1360'] = balance.get('current1360')
            org_info[f'{int(self.year) - 1}_1360'] = balance.get('previous1360')
            org_info[f'{int(self.year) - 2}_1360'] = balance.get('beforePrevious1360')
            # Нераспределенная прибыль (непокрытый убыток)
            org_info[f'{self.year}_1370'] = balance.get('current1370')
            org_info[f'{int(self.year) - 1}_1370'] = balance.get('previous1370')
            org_info[f'{int(self.year) - 2}_1370'] = balance.get('beforePrevious1370')
            # Итого по разделу III
            org_info[f'{self.year}_1300'] = balance.get('current1300')
            org_info[f'{int(self.year) - 1}_1300'] = balance.get('previous1300')
            org_info[f'{int(self.year) - 2}_1300'] = balance.get('beforePrevious1300')
            # IV. Долгосрочные обязательства
            # V. Краткосрочные обязательства
            # Заемные средства
            org_info[f'{self.year}_1510'] = balance.get('current1510')
            org_info[f'{int(self.year) - 1}_1510'] = balance.get('previous1510')
            org_info[f'{int(self.year) - 2}_1510'] = balance.get('beforePrevious1510')
            # Кредиторская задолженность
            org_info[f'{self.year}_1520'] = balance.get('current1520')
            org_info[f'{int(self.year) - 1}_1520'] = balance.get('previous1520')
            org_info[f'{int(self.year) - 2}_1520'] = balance.get('beforePrevious1520')
            # Доходы будущих периодов
            org_info[f'{self.year}_1530'] = balance.get('current1530')
            org_info[f'{int(self.year) - 1}_1530'] = balance.get('previous1530')
            org_info[f'{int(self.year) - 2}_1530'] = balance.get('beforePrevious1530')
            # Оценочные обязательства
            org_info[f'{self.year}_1540'] = balance.get('current1540')
            org_info[f'{int(self.year) - 1}_1540'] = balance.get('previous1540')
            org_info[f'{int(self.year) - 2}_1540'] = balance.get('beforePrevious1540')
            # Прочие обязательства
            org_info[f'{self.year}_1550'] = balance.get('current1550')
            org_info[f'{int(self.year) - 1}_1550'] = balance.get('previous1550')
            org_info[f'{int(self.year) - 2}_1550'] = balance.get('beforePrevious1550')
            # Итого по разделу V
            org_info[f'{self.year}_1500'] = balance.get('current1500')
            org_info[f'{int(self.year) - 1}_1500'] = balance.get('previous1500')
            org_info[f'{int(self.year) - 2}_1500'] = balance.get('beforePrevious1500')
            # Баланс по ПАССИВЫ
            org_info[f'{self.year}_1700'] = balance.get('current1700')
            org_info[f'{int(self.year) - 1}_1700'] = balance.get('previous1700')
            org_info[f'{int(self.year) - 2}_1700'] = balance.get('beforePrevious1700')

            # fin_res
            # Выручка
            org_info[f'{self.year}_2110'] = fin_res.get('current2110')
            org_info[f'{int(self.year) - 1}_2110'] = fin_res.get('previous2110')
            # Себестоимость продаж
            org_info[f'{self.year}_2120'] = fin_res.get('current2120')
            org_info[f'{int(self.year) - 1}_2120'] = fin_res.get('previous2120')
            # Валовая прибыль (убыток)
            org_info[f'{self.year}_2100'] = fin_res.get('current2100')
            org_info[f'{int(self.year) - 1}_2100'] = fin_res.get('previous2100')
            # Управленческие расходы
            org_info[f'{self.year}_2220'] = fin_res.get('current2220')
            org_info[f'{int(self.year) - 1}_2220'] = fin_res.get('previous2220')
            # Прибыль (убыток) от продаж
            org_info[f'{self.year}_2200'] = fin_res.get('current2200')
            org_info[f'{int(self.year) - 1}_2200'] = fin_res.get('previous2200')
            # Проценты к уплате
            org_info[f'{self.year}_2330'] = fin_res.get('current2330')
            org_info[f'{int(self.year) - 1}_2330'] = fin_res.get('previous2330')
            # Прочие доходы
            org_info[f'{self.year}_2340'] = fin_res.get('current2340')
            org_info[f'{int(self.year) - 1}_2340'] = fin_res.get('previous2340')
            # Прочие расходы
            org_info[f'{self.year}_2350'] = fin_res.get('current2350')
            org_info[f'{int(self.year) - 1}_2350'] = fin_res.get('previous2350')
            # Прибыль (убыток) до налогообложения
            org_info[f'{self.year}_2300'] = fin_res.get('current2300')
            org_info[f'{int(self.year) - 1}_2300'] = fin_res.get('previous2300')
            # Налог на прибыль
            org_info[f'{self.year}_2410'] = fin_res.get('current2410')
            org_info[f'{int(self.year) - 1}_2410'] = fin_res.get('previous2410')
            #     # в т.ч. текущий налог на прибыль
            #     org_info[f'{self.year}_2411'] = fin_res.get('current2411')
            #     org_info[f'{int(self.year) - 1}_2411'] = fin_res.get('previous2411')
            #     # Отложенный налог на прибыль
            #     org_info[f'{self.year}_2412'] = fin_res.get('current2412')
            #     org_info[f'{int(self.year) - 1}_2412'] = fin_res.get('previous2412')
            # Чистая прибыль (убыток)
            org_info[f'{self.year}_2400'] = fin_res.get('current2400')
            org_info[f'{int(self.year) - 1}_2400'] = fin_res.get('previous2400')
            # Совокупный финансовый результат периода (не для упрощённой формы)
            org_info[f'{self.year}_2500'] = fin_res.get('current2500')
            org_info[f'{int(self.year) - 1}_2500'] = fin_res.get('previous2500')

            item = BfoparserItem(org_info=org_info)
            yield item
