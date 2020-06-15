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

    def process(self):
        """
            1. connection
            2. get html source
        :return:
        """
        self.connection()
        a = self.driver.find_elements_by_class_name("difficulty")
        for e in a:
            print(e.text)
            e.click()


class HaemukCrawler:
    def __init__(self):
        pass

    def process(self):
        pass
