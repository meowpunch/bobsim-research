from crawler.ancestor import SeleniumCrawler


class ItemCrawler(SeleniumCrawler):
    """
        crawl item categories
    """

    def __init__(self, base_url, bucket_name, key, head):
        super().__init__(base_url, bucket_name, key, head)

    def process(self):
        """
            1. travel
        :return: items
        """
        return self.crawl()

    def crawl(self):
        """
            event(click) <-> get_items
        :return: items
        """
        pass


class HaemukItemCrawler(ItemCrawler):

    def __init__(self, base_url, bucket_name, key, head):
        super().__init__(base_url, bucket_name, key, head)

    def process(self):
        """
            1. travel
        :return: items
        """
        return self.crawl()

    def connection(self) -> None:
        self.driver.get(self.base_url)
        self.logger.debug("success to connect with '{url}'".format(url=self.base_url))

    def crawl(self):
        """
            1. connection
            2. get parents
            3. make tuple (parent, children)
            4. return dict

            parent:
                child...

        :return: items
        """
        self.connection()
        parents = self.driver.find_element_by_class_name('big_sort').find_elements_by_tag_name('a')

        return dict(map(self.get_item_categories, parents))

    def get_item_categories(self, parent):
        parent.click()
        self.driver.implicitly_wait(3)
        children = self.driver.find_element_by_class_name('small_sort').text.split("\n")

        return parent.text, children
