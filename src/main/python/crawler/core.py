from collections import OrderedDict
from urllib.error import HTTPError
from selenium.common.exceptions import UnexpectedAlertPresentException, NoSuchElementException
import urllib.request

from selenium import webdriver

from util.logging import init_logger


class RecipeCrawler:
    def __init__(self):
        pass

    def connection(self):
        pass

    def process(self):
        pass


class MangeCrawler(RecipeCrawler):
    def __init__(self, url="https://www.10000recipe.com/recipe/"):
        self.logger = init_logger()

        self.chrome_path = "C:/chromedriver"
        self.driver = webdriver.Chrome(self.chrome_path)

        self.base_url = url
        super().__init__()

    # @property
    # def driver(self):
    #     return webdriver.Chrome(self.chrome_path)

    def process(self):
        """
            1. connection
            2. get html source
        :return: recipe
        """
        try:
            self.connection()
            self.driver.implicitly_wait(3)
            recipe = self.get_recipe()

        except HTTPError as e:
            self.logger.exception(e, exc_info=True)
            return False

        except ValueError as e1:
            self.logger.exception(e1, exc_info=True)
            return False

        except TypeError as e2:
            self.logger.exception(e2, exc_info=True)
            return False

        except NoSuchElementException as e3:
            self.logger.exception(e3, exc_info=True)
            return False

        except UnexpectedAlertPresentException as e4:
            self.logger.exception(e4, exc_info=True)
            return False

        return True

    def connection(self, recipe_num=6847470):
        target_url = self.base_url + str(recipe_num)
        self.driver.get(target_url)
        self.logger.debug("success to connect with '{url}'".format(url=target_url))

    def get_recipe(self):
        """

        :return: recipe dictionary
        """
        self.logger.info("start!")
        recipe_step = []
        recipe_title = self.driver.find_element_by_class_name("view2_summary").find_element_by_tag_name("h3").text
        self.logger.info(recipe_title)
        self.logger.info(recipe_title.text)
        recipe_source = self.driver.find_element_by_class_name("ready_ingre3").text.split('\n')
        consumed_time = self.driver.find_element_by_class_name("view2_summary_info2").text
        proportion_of_food = self.driver.find_element_by_class_name("view2_summary_info1").text
        difficulty = self.driver.find_element_by_class_name("view2_summary_info3").text
        recipe_tags = self.driver.find_element_by_class_name("view_tag").text.split("#")
        recipe_tags.remove('')
        num = 1

        # while True:
        #     try:
        #         recipe_step.append(self.driver.find_element_by_id("stepdescr" + str(num)).text)
        #         num += 1
        #     except NoSuchElementException as e:
        #         break
        # img = self.driver.find_element_by_class_name('centeredcrop').find_element_by_id("main_thumbs").get_attribute("src")
        # urllib.request.urlretrieve(img, "./images/" + "test" + ".png")
        recipe = OrderedDict()
        recipe["제목"] = recipe_title
        recipe["재료"] = recipe_source
        recipe["조리방법"] = recipe_step
        recipe["소요시간"] = consumed_time
        recipe["난이도"] = difficulty
        recipe["양"] = proportion_of_food
        recipe["태그"] = recipe_tags
        print(recipe)
        return True


class HaemukCrawler:
    def __init__(self):
        pass

    def process(self):
        pass
