from urllib.error import HTTPError
from selenium.common.exceptions import UnexpectedAlertPresentException, NoSuchElementException
import urllib.request

from selenium import webdriver


class RecipeCrawler:
    def __init__(self):
        pass

    def connection(self):
        pass

    def process(self):
        pass


class MangeCrawler(RecipeCrawler):
    def __init__(self, url="https://haemukja.com/recipes/"):
        self.base_url = url
        self.chrome_path = "C:/chromedriver"
        super().__init__()

    @property
    def driver(self):
        return webdriver.Chrome(self.chrome_path)

    def connection(self):
        self.driver.get(self.base_url)
        return True

    @property
    def process(self):
        """
            1. connection
            2. get html source
        :return:
        """
        try:
            self.connection()
            a = self.driver.find_elements_by_class_name("difficulty")

        except HTTPError as e:
            # print(e.msg)
            return False

        except ValueError as e1:
            # print(e1)
            return False

        except TypeError as e2:
            # print(e2)
            return False

        except NoSuchElementException:
            # print(e3.msg)
            return False

        except UnexpectedAlertPresentException:
            # print(e3.msg)
            return False

        return True


class HaemukCrawler:
    def __init__(self):
        pass

    def process(self):
        pass
