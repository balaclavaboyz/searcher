from scrapy.utils.project import get_project_settings
import shutil
import pandas
import csv
import re
from scrapy.loader import ItemLoader
from ml.items import MlItem
import os
import json
import scrapy
from scrapy.http import FormRequest
from scrapy.crawler import CrawlerProcess
from dotenv import load_dotenv

load_dotenv()
cnpj=os.environ.get("cnpj")
senha=os.environ.get("senha")

class search_ml(scrapy.Spider):
    # custom_settings={
    #     'AUTOTHROTTLE_ENABLED': True,
    #     'ROBOTSTXT_OBEY' : False,
    #     'USER_AGENT' : 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/109.0.0.0 Safari/537.36'
    # }

    name = 'search_ml'

    # def __init__(self, context=None, unit=None):
    #     self.context = context
    #     self.unit = unit
    def __init__(self):
        self.urls=[]
        with open('hayamax.json','r',encoding='utf-8') as qwe:
            tempdata=json.load(qwe)
            for i in tempdata:
                title_without_slash = re.sub('/', ' ', i['title'])
                # # escape ""
                # title_without_slash = re.sub('"', '', title_without_slash)
                # t.write(title_without_slash+'\n')

                # escape as well %
                title_without_slash = re.sub('%', '%%', title_without_slash)
                search_term = re.sub(' ', '-', title_without_slash)
                newlink='https://lista.mercadolivre.com.br/'+search_term+'_OrderId_PRICE_NoIndex_True'
                self.urls.append(newlink)

    def start_requests(self):
        for url in self.urls:
            yield scrapy.Request(url=url, callback=self.parse)

    def parse(self, res):

        # check if the list of products exists
        for i in res.xpath('//*[@id="root-app"]/div/div[2]/section/ol'):
            l = ItemLoader(item=MlItem(), selector=i)

            l.add_xpath(
                'price', './/span[@class="price-tag-text-sr-only"]/text()')
            l.add_xpath(
                'title', '//*[@id="root-app"]/div/div[2]/section/ol/li[1]/div/div/div[2]/div[1]/a[1]/h2/text()')
            l.add_xpath(
                'link', '//*[@id="root-app"]/div/div[2]/section/ol/li[1]/div/div/div[2]/div[1]/a/@href')

            # if xpath of title return none than
            if i.xpath('.//h2[@class="ui-search-item__title ui-search-item__group__element shops__items-group-details shops__item-title"]/text()'):
                l.add_xpath(
                    'title', './/h2[@class="ui-search-item__title ui-search-item__group__element shops__items-group-details shops__item-title"]/text()')

            # same thing
            if i.xpath('.//h2[@class="ui-search-item__title ui-search-item__group__element shops__items-group-details shops__item-title"]/text()'):
                l.add_xpath(
                    'link', './/div[@class="andes-card andes-card--flat andes-card--default ui-search-result shops__cardStyles ui-search-result--core andes-card--padding-default andes-card--animated"]/a/@href')

            yield l.load_item()


class HayamaxSpider(scrapy.Spider):
    name = 'hayamax'

    def start_requests(self):
        login_url = 'https://loja.hayamax.com.br/entrar-cliente'
        yield scrapy.Request(meta={'dont_redirect': True, 'handle_httpstatus_list': [302]}, url=login_url, callback=self.login, dont_filter=True)

    def login(self, res):
        yield FormRequest.from_response(res, formid="form-login", formdata={'customer[stcd1]': cnpj, 'customer[password]': senha}, callback=self.after_login)

    def after_login(self, res):
        if b"seja bem vindo" in res.body:
            print('login ok')
        else:
            print('login error')
            return
        with open('hayamax_links.txt','r',encoding='utf-8') as f:
            for t in f:
                print(t)
                yield scrapy.Request(
                    meta={
                        'dont_redirect': True,
                        'handle_httpstatus_list': [302]
                    }, url=t, callback=self.parsedata)

    def parsedata(self, res):
        for i in res.xpath('//*[@class="search-product"]'):
            l = ItemLoader(item=MlItem(), selector=i)
            if i.xpath('.//*[@class="search-product-price spp-color-hayamax"]/span/text()') == ' ':
                print('price achou nada')
                l.add_xpath('price', '0')
            else:
                print('price achou algo')
                print(i.xpath('.//*[@class="search-product-price spp-color-hayamax"]/span/text()'))
                l.add_xpath(
                    'price', './/*[@class="search-product-price spp-color-hayamax"]/span/text()')
            # item['price']= i.xpath(
            #     './/*[@class="search-product-price spp-color-hayamax"]/span/text()').get()
            l.add_xpath('title', './/*[@class="search-product-title"]/text()')
            l.add_xpath('link', './/div[@class="col-12 mx-auto"]/a/@href')
            # item['title']=i.xpath('.//*[@class="search-product-title"]/text()').get()
            # item['link']='https://loja.hayamax.com.br/'+i.xpath('.//div[@class="col-12 mx-auto"]/a/@href').get()
            l.add_xpath(
                'unit', './/*[@class="row search-product-divall"]/div/p[2]/text()')

            yield l.load_item()

class start(scrapy.Spider):
    name = 'start'

    def __init__(self):
        self.hayamax_search_in_ml()
        # self.filter_hayamax_products()

    def hayamax_search_in_ml(self):
        if os.path.exists('./output'):
            shutil.rmtree('./output')
        with open('hayamax.json', 'r', encoding="utf-8") as f:
            with open('titles_hayamax.txt','w',encoding='utf-8') as t:
                data = json.load(f)
                for item in data:
                    title_without_slash = re.sub('/', ' ', item['title'])
                    # escape ""
                    title_without_slash = re.sub('"', '', title_without_slash)
                    t.write(title_without_slash+'\n')
                    # escape as well %
                    title_without_slash = re.sub('%', '%%', title_without_slash)

                    process = CrawlerProcess({
                        'COOKIES_ENABLED': 'False',
                        'DOWNLOAD_DELAY':2,
                        'CONCURRENT_REQUESTS' : 1,
                        'RETRY_ENABLED' : 'True',
                        'DOWNLOAD_FAIL_ON_DATALOSS' : 'false',
                        'AUTOTHROTTLE_ENABLED' : 'True',
                        'TELNETCONSOLE_ENABLED' : 'False',
                        'USER_AGENT' : 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/109.0.0.0 Safari/537.36',
                        'FEED_FORMAT': 'csv',
                        'FEED_URI': f"./output/{title_without_slash}.csv",
                        'USER_AGENT': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/109.0.0.0 Safari/537.36'
                    })
                    process.crawl(search_ml, context=item['title'], unit=item['unit'])
        process.start()

    def formula(self,original_price):
        margem = 0.00
        simples_nacional = 0.04
        comissao_ml = 0.13
        cmv = original_price*1.06

        if cmv >= 79:
            taxa_fixa = 19.5
            preco_de_venda = (cmv+taxa_fixa) / \
                (1-comissao_ml - simples_nacional - margem)
            lucro_bruto = preco_de_venda-cmv-simples_nacional * \
                preco_de_venda-comissao_ml*preco_de_venda-taxa_fixa
            return preco_de_venda
        else:
            taxa_fixa = 5.5
            preco_de_venda = (cmv+taxa_fixa) / \
                (1-comissao_ml - simples_nacional - margem)
            lucro_bruto = preco_de_venda-cmv-simples_nacional * \
                preco_de_venda-comissao_ml*preco_de_venda-taxa_fixa
            return preco_de_venda
        return -1

    def filter_hayamax_products(self):
        list_of_products=[]
        with open('titles_hayamax.txt','r',encoding='utf-8') as f:
            lines=f.readlines()
            for i in lines:
                list_of_products.append(i.strip())
        with open('list.csv', 'w', encoding='utf-8') as output_file:  # resultado em uma lista txt
            # header for csv file
            csv_header = [
                'title',
                'q',
                'margin',
                'price_hayamax',
                'price_hayamax_formula',
                'link_hayamax',
                'price_ml',
                'link_ml'
            ]
            writer = csv.DictWriter(output_file, fieldnames=csv_header)
            writer.writeheader()
            with open('hayamax.json', 'r', encoding='utf-8') as hayamax_file:  # open json distribuidora
                hayamax_file_toread=hayamax_file.read()
                for item in list_of_products:
                    if os.stat('./output/'+item+'.csv').st_size != 0:
                        # for line in hayamax_list:
                        name_of_product = re.sub('.csv', '', item)
                        # name_of_product = re.sub('-', '/', name_of_product)
                        print(name_of_product)
                        if name_of_product in hayamax_file_toread:

                            df_individual_hayamax_item = pandas.read_csv(
                                './output/'+item+'.csv')
                            df_hayamax = pandas.read_json('hayamax.json')
                            result_query_hayamax = df_hayamax.query(
                                f'title=="{name_of_product}"')

                            hayamax_price = result_query_hayamax['price'].values[0]
                            hayamax_price_with_dot = re.sub(
                                ',', '.', hayamax_price)
                            hayamax_price_with_dot_without_symbol = re.sub(
                                'R\$', ' ', hayamax_price_with_dot)
                            if hayamax_price_with_dot_without_symbol == '\xa0':
                                print('empty price')
                            elif(df_individual_hayamax_item['title'].isnull().any().any()):
                                print('no title')
                            else:
                                quocient = re.findall(
                                    '\d+', result_query_hayamax['unit'].values[0])[0]

                                try:
                                    print(float(hayamax_price_with_dot_without_symbol)/float(quocient))
                                    price_after_formula = self.formula(
                                        float(hayamax_price_with_dot_without_symbol)/float(quocient)
                                        )
                                    print(price_after_formula)
                                except:
                                    price_after_formula = 0

                                list_of_prices_ml = df_individual_hayamax_item['price'].to_list(
                                )
                                list_of_prices_fixed = []

                                for value in list_of_prices_ml:
                                    # if in price, found 'reais con' replace with .
                                    tmp3 = re.sub(' reais con ', '.', value)
                                    # if find centavos, delete
                                    tmp4 = re.sub(' centavos', '', tmp3)
                                    # if find reais, delete
                                    tmp5 = re.sub(' reais', '', tmp4)
                                    # if find 'Antes: ',delete
                                    tmp6 = re.sub('Antes: ', '', tmp5)
                                    list_of_prices_fixed.append(tmp6)
                                best_price_ml = min(list_of_prices_fixed, key=float)
                                print(list_of_prices_fixed)
                                print(best_price_ml)

                                margin = 0


                                if price_after_formula != 0:
                                    res = (
                                        (float(price_after_formula)/float(best_price_ml))-1)
                                    # print(price_after_formula)
                                    # print(best_price_ml)
                                    # print(res)
                                    margin = "%.6f" % res
                                if price_after_formula < float(best_price_ml):
                                    context = {
                                        'title': re.sub('/',' ',df_individual_hayamax_item['title'][0]),
                                        'q':quocient,
                                        'margin': margin,
                                        'price_hayamax': hayamax_price_with_dot_without_symbol,
                                        'price_hayamax_formula': price_after_formula,
                                        'link_hayamax': 'https://loja.hayamax.com.br'+result_query_hayamax['link'].values[0],
                                        'price_ml': best_price_ml,
                                        'link_ml': df_individual_hayamax_item['link'][0]
                                    }
                                    writer.writerow(context)

                                print('done')
                        else:
                            print('doesnt exists')
