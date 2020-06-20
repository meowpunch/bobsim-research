from crawler.ancestor import SeleniumCrawler


class ItemCrawler(SeleniumCrawler):
    """
        crawl item categories
    """

    def __init__(self, base_url, bucket_name, key):
        super().__init__(base_url, bucket_name, key)

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
