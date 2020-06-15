from collections import OrderedDict
from urllib.error import HTTPError
from selenium.common.exceptions import UnexpectedAlertPresentException, NoSuchElementException
import urllib.request

from selenium import webdriver

from util.logging import init_logger


class RecipeCrawler:
    def __init__(self, base_url, candidate_num, recipe_dict):
        self.logger = init_logger()

        self.chrome_path = "C:/chromedriver"
        self.driver = webdriver.Chrome(self.chrome_path)
        self.driver.implicitly_wait(3)

        self.base_url = base_url
        self.candidate_num = candidate_num

        self.recipe_dict = recipe_dict

    def process(self):
        result_code = list(map(lambda n: self.crawl_recipe(recipe_num=n), self.candidate_num))
        success = sum(result_code)
        fail = len(result_code) - success

        self.logger.info("success: {s},  fail: {f}".format(s=success, f=fail))
        self.driver.quit()
        return result_code

    def crawl_recipe(self, recipe_num):
        """
            1. connection
            2. get recipe
            3. convert to json
            4. save
        :return: exit code
        """
        try:
            self.connection(recipe_num=recipe_num)
            # self.driver.implicitly_wait(3)
            recipe = self.get_recipe()
            self.logger.info(recipe)

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
            # TODO: NoSuchElementException Handling
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
        pass


class MangeCrawler(RecipeCrawler):
    def __init__(self):
        """
            https://www.10000recipe.com/recipe/recipe_num
            recipe_num: about 6828805 ~ 6935000

            record:
                            total_recipe
                15.06.2020: 138,873
        """
        super().__init__(
            base_url="https://www.10000recipe.com/recipe/",
            candidate_num=range(6828805, 6828820),
            recipe_dict=selector.mange
        )

    def select_element(self):
        return {
            "title": self.driver.find_element_by_tag_name("h3").text,
            "description": self.driver.find_element_by_class_name("view2_summary_in_m2").text,
            "views": self.driver.find_element_by_class_name("hit font_num").text,
            "time": self.driver.find_element_by_class_name("view2_summary_info2").text,
            "person": self.driver.find_element_by_class_name("view2_summary_info1").text,
            "difficulty": self.driver.find_element_by_class_name("view2_summary_info3").text,
            "items": self.driver.find_element_by_class_name("ready_ingre3").text,  # .split('\n'),
            "steps": None,
            "caution": self.driver.find_element_by_class_name("view_step_tip").find,
            "writer": None,
            "comments": None,
            "tag": None,
        }


    def get_recipe(self):
        """
        :return: recipe dictionary
        """
        # TODO: refactoring

        # recipe_step = []
        # recipe_title = self.driver.find_element_by_class_name("view2_summary").find_element_by_tag_name("h3").text
        # self.logger.info("title: {}".format(recipe_title))
        # recipe_source = self.driver.find_element_by_class_name("ready_ingre3").text.split('\n')
        # consumed_time = self.driver.find_element_by_class_name("view2_summary_info2").text
        # proportion_of_food = self.driver.find_element_by_class_name("view2_summary_info1").text
        # difficulty = self.driver.find_element_by_class_name("view2_summary_info3").text
        # recipe_tags = self.driver.find_element_by_class_name("view_tag").text.split("#")
        # recipe_tags.remove('')
        # num = 1

        # while True:
        #     try:
        #         recipe_step.append(self.driver.find_element_by_id("stepdescr" + str(num)).text)
        #         num += 1
        #     except NoSuchElementException as e:
        #         break
        # img = self.driver.find_element_by_class_name('centeredcrop').find_element_by_id("main_thumbs").get_attribute("src")
        # urllib.request.urlretrieve(img, "./images/" + "test" + ".png")
        # recipe = OrderedDict()
        # recipe["제목"] = recipe_title
        # recipe["재료"] = recipe_source
        # recipe["조리방법"] = recipe_step
        # recipe["소요시간"] = consumed_time
        # recipe["난이도"] = difficulty
        # recipe["양"] = proportion_of_food
        # recipe["태그"] = recipe_tags
        return recipe


class HaemukCrawler(RecipeCrawler):
    def __init__(self):
        """
        """
        super().__init__(
            base_url="https://www.haemukja.com/recipes/",
            candidate_num=range(5000, 5001),
            recipe_dict=None
        )

    def get_recipe(self):
        # TODO: test get_recipe by HANK
        x = self.driver.find_element_by_class_name("top").find_element_by_tag_name("h1")
        print(x)
        print(x.text)
