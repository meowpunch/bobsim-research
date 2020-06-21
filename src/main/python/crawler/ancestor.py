from selenium import webdriver

from utils.logging import init_logger
from utils.s3_manager.manage import S3Manager


class SeleniumCrawler:
    def __init__(self, base_url, bucket_name, key, head=False):
        self.logger = init_logger()

        self.bucket_name = bucket_name
        self.s3_manager = S3Manager(bucket_name=self.bucket_name)
        self.prefix = key

        self.chrome_path = "C:/chromedriver"
        options = webdriver.ChromeOptions()
        if head is False:
            options.add_argument('headless')

        self.driver = webdriver.Chrome(executable_path=self.chrome_path, chrome_options=options)

        self.base_url = base_url

    def click_element_by_xpath(self, xpath: str):
        ele = self.driver.find_element_by_xpath(xpath=xpath)
        ele.click()

    def click_element_by_class_name(self, name: str):
        ele = self.driver.find_element_by_class_name(name=name)
        ele.click()

    def click_by_tag_name(self, name: str):
        ele = self.driver.find_element_by_tag_name(name=name)
        ele.click()

    def process(self):
        pass


