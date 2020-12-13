from selenium import webdriver
from selenium.webdriver.common.keys import Keys
import time
import json
import re
from openpyxl import Workbook
import threading


class CatchJD(object):

    def __init__(self, **kwargs):
        l_time = time.localtime(time.time())
        self.run_flag = False
        self.get = kwargs.get('get', 'https://www.jd.com')
        self.goods_list = list()
        self.goods_name = ""
        self.filename = f'catch_{l_time.tm_mon}{l_time.tm_mday}_{l_time.tm_hour}{l_time.tm_min}'
        self.drivers = None
        self.is_close = True

    def get_good(self):
        try:

            # 通过JS控制滚轮滑动获取所有商品信息
            js_code = '''
                window.scrollTo(0,5000);
            '''
            self.drivers.execute_script(js_code)  # 执行js代码

            # 等待数据加载
            time.sleep(2)

            # 3、查找所有商品div
            # good_div = driver.find_element_by_id('J_goodsList')
            good_list = self.drivers.find_elements_by_class_name('gl-item')
            n = 1

            for good in good_list:
                # 根据属性选择器查找
                # 商品链接
                good_url = good.find_element_by_css_selector(
                    '.p-img a').get_attribute('href')

                # 商品名称
                good_name = good.find_element_by_css_selector(
                    '.p-name em').text.replace("\n", "--")

                # 商品价格
                price_txt = good.find_element_by_class_name(
                    'p-price').text.replace("\n", ":")
                good_price = re.search(r"\d+(\.?\d+)", price_txt).group()

                # 评价人数
                good_commit = good.find_element_by_class_name(
                    'p-commit').text.replace("\n", " ")

                # good_content = f'{good_url}\t{good_name}\t{good_price}\t{good_commit}\n'
                # print(good_content)
                # sql_valus.append(())
                # with open('jd.txt', 'a', encoding='utf-8') as f:
                #     f.write(good_content)
                dp = (good_url, good_name, good_price, good_commit)
                self.goods_list.append(dp)

                # print(dp)
                with open(f'{self.filename}.line.data', 'a', encoding='utf-8')as sql:
                    sql.write(json.dumps(dp) + '\n')

                # with open(f'{self.filename}.csv', 'a', encoding='utf-8')as f:
                #     f.write(good_content)

            next_tag = self.drivers.find_element_by_class_name('pn-next')
            next_tag.click()

            if self.is_close:
                return
            else:
                time.sleep(2)
                self.get_good()
                time.sleep(10)

        except Exception as ret:
            print(ret)
            self.drivers.close()
            self.run_flag = False
            self.is_close = True
        # finally:

    def run(self):
        self.run_flag = True
        self.is_close = False

        self.drivers = webdriver.Chrome()
        self.drivers.implicitly_wait(10)

        # print(self.goods_name, not self.goods_name)
        # return
        # # 1、往京东主页发送请求
        self.drivers.get(
            self.get or 'https://www.jd.com')
        input_tag = self.drivers.find_element_by_id('key')
        input_tag.send_keys(self.goods_name)
        input_tag.send_keys(Keys.ENTER)
        time.sleep(2)

        self.T1 = threading.Thread(target=self.get_good)
        self.T1.start()
        pass

    def close(self):
        self.is_close = True
        self.run_flag = False
        with open(f'{self.filename}.data', 'w') as f_d:
            json.dump(self.goods_list, f_d)
        time.sleep(5)
        self.drivers.close()
        pass

    def exit(self):
        pass


if __name__ == '__main__':
    # url = 'https://search.jd.com/search?keyword=iPhone&qrst=1&wq=iPhone&shop=1&stock=1&stock=1&ev=exbrand_Apple%5E'

    catchs = CatchJD()

    driver = None
    # good_name = input('请输入爬取商品信息:').strip()
    while True:
        ctrl = int(input("1,启动抓取;\n3,停止抓取;\n0退出：\n"))

        if ctrl == 3:
            catchs.close()
        elif ctrl == 1:
            catchs.goods_name = input("输入要抓取的关键字")
            catchs.run()
        elif ctrl == 0:
            catchs.exit()
            break
