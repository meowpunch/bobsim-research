from selenium import webdriver

from utils.logging import init_logger
from utils.s3_manager.manage import S3Manager


class SeleniumCrawler:
    def __init__(self, base_url, bucket_name, key):
        self.logger = init_logger()

        self.bucket_name = bucket_name
        self.s3_manager = S3Manager(bucket_name=self.bucket_name)
        self.prefix = key

        self.chrome_path = "C:/chromedriver"
        options = webdriver.ChromeOptions()
        options.add_argument('headless')

        self.driver = webdriver.Chrome(executable_path=self.chrome_path, chrome_options=options)

        self.base_url = base_url

    def process(self):
        pass
