from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions
from selenium.webdriver.support.wait import WebDriverWait

from crawler.ancestor import SeleniumCrawler


class ItemCrawler(SeleniumCrawler):
    """
        To crawl item categories

        - For Cooking
            Based on recipe providing service

        - For Shelf Life Management
            Based on online grocery shop
    """

    def __init__(self, base_url, bucket_name, key, head):
        super().__init__(base_url, bucket_name, key, head)

    def process(self) -> dict:
        """
            1. travel
        :return: items
        """
        return self.crawl()

    def crawl(self) -> dict:
        """
            1. connection
            2. get item_categories
        :return: item_categories
        """
        self.connection()
        return self.get_item_categories()

    def connection(self) -> None:
        self.driver.get(self.base_url)
        self.logger.debug("success to connect with '{url}'".format(url=self.base_url))

    def get_item_categories(self) -> dict:
        """
            event(click) <-> get_items
        """
        pass


class HaemukItemCrawler(ItemCrawler):
    """
        - Recipe Providing Service-
        https://www.haemukja.com/refrigerator

        parent - children
    """

    def __init__(self, base_url, bucket_name, key, head):
        super().__init__(base_url, bucket_name, key, head)

    def get_item_categories(self) -> dict:
        """
            parent:
                children
        :return: item categories
        """
        parents = self.driver.find_element_by_class_name('big_sort').find_elements_by_tag_name('a')

        def make_tuple(parent):
            parent.click()
            WebDriverWait(self.driver, 3).until(
                expected_conditions.presence_of_element_located(
                    (By.XPATH, '//*[@id="container"]/div[2]/section/div/form/fieldset[1]/ul[2]/li')
                )
            )
            children = self.driver.find_element_by_class_name('small_sort').text.split("\n")

            return parent.text, children

        return dict(map(make_tuple, parents))


class EmartItemCrawler(ItemCrawler):
    """
        - Online Grocery Shop -
         http://emart.ssg.com/

         grand parent - parent - children
    """

    def __init__(self, base_url, bucket_name, key, head):
        super().__init__(base_url, bucket_name, key, head)

    def get_item_categories(self):
        """
            grand parent:
                    parent:
                         children
        :return:
        """
        grand_parents = self.driver.find_element_by_class_name('em_lnb_lst').find_elements_by_tag_name('a')


        pass
