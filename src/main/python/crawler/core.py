from collections import OrderedDict
from urllib.error import HTTPError

from selenium import webdriver
from selenium.common.exceptions import UnexpectedAlertPresentException, NoSuchElementException

from util.logging import init_logger
from util.s3_manager.manage import S3Manager


class RecipeCrawler:
    def __init__(self, base_url, candidate_num):
        self.logger = init_logger()
        self.s3_manager = S3Manager(bucket_name="production-bobsim")

        self.chrome_path = "C:/chromedriver"
        self.driver = webdriver.Chrome(self.chrome_path)
        self.driver.implicitly_wait(3)

        self.base_url = base_url
        self.candidate_num = candidate_num

    def process(self):
        result_code = list(map(lambda n: self.crawl_recipe(recipe_num=n), self.candidate_num))
        # success = sum(result_code)
        # fail = len(result_code) - success
        #
        # self.logger.info("success: {s},  fail: {f}".format(s=success, f=fail))
        self.driver.quit()
        return result_code

    def crawl_recipe(self, recipe_num):
        """
            1. connection
            2. get recipe:dict
            4. save_to_json
        :return: recipe(Success) or False(Fail)
        """
        try:
            self.connection(recipe_num=recipe_num)
            self.driver.implicitly_wait(3)

            recipe = self.get_recipe()
            self.logger.info(recipe)

            self.save(recipe=recipe, recipe_num=recipe_num)
            return recipe

        except HTTPError as e:
            self.logger.exception(e, exc_info=True)
            return False

        except ValueError as e:
            self.logger.exception(e, exc_info=True)
            return False

        except TypeError as e:
            self.logger.exception(e, exc_info=True)
            return False

        except UnexpectedAlertPresentException as e:
            self.logger.exception(e, exc_info=True)
            return False

        except NotImplementedError as e:
            self.logger.exception(e, exc_info=True)
            return False

    def connection(self, recipe_num=6847470):
        target_url = self.base_url + str(recipe_num)
        self.driver.get(target_url)
        self.logger.debug("success to connect with '{url}'".format(url=target_url))

    def save(self, recipe: dict, recipe_num=6847470):
        self.s3_manager.save_dict_to_json(data=recipe, key="crawled_recipe/{num}.json".format(num=recipe_num))

    def get_recipe(self):
        """
            return type should be dict when overridden
        """
        raise NotImplementedError


class MangeCrawler(RecipeCrawler):
    def __init__(self):
        """
            https://www.10000recipe.com/recipe/recipe_num
            recipe_num: about 6828805 ~ 6935000

            record:
                            total_recipe
                15.06.2020: 138,873
        """
        self.field = ['title', 'description', 'views', 'time', 'person', 'difficulty',
                      'items', 'steps', 'caution', 'writer', 'comments', 'tag']

        super().__init__(
            base_url="https://www.10000recipe.com/recipe/",
            candidate_num=range(6828809, 6828811),
        )

    def get_recipe(self):
        """
        :return: recipe: dict
        """
        return OrderedDict(map(self.make_tuple, self.field))

    def make_tuple(self, key):
        """
        :param key: key
        :return: (key, value)
        """
        try:
            # self.logger.info("({}, {})".format(key, self.select_element(key)))
            return key, self.select_element(key)
        except NoSuchElementException:
            self.logger.info("No Element about '{k}'".format(k=key), exc_info=True)
            return key, None
        except KeyError:
            raise NotImplementedError

    def select_element(self, key):
        return {
            "title": lambda d: d.find_element_by_tag_name("h3").text,
            "description": lambda d: d.find_element_by_class_name("view2_summary_in").text,
            "views": lambda d: d.find_element_by_class_name("hit").text,
            "time": lambda d: d.find_element_by_class_name("view2_summary_info2").text,
            "person": lambda d: d.find_element_by_class_name("view2_summary_info1").text,
            "difficulty": lambda d: d.find_element_by_class_name("view2_summary_info3").text,
            "items": lambda d: d.find_element_by_class_name("ready_ingre3").text.split("\n"),
            "steps": lambda d: None,
            "caution": lambda d: d.find_element_by_class_name("view_step_tip").text,
            "writer": lambda d: d.find_element_by_class_name("profile_cont").text.split("\n"),
            # TODO: count star of reviews
            "comments": lambda d: d.find_element_by_class_name("view_reply").text.split("\n"),
            "tag": lambda d: d.find_element_by_class_name("view_tag").text.split("#"),
        }[key](self.driver)

    def save(self, recipe: dict, recipe_num=6847470):
        self.s3_manager.save_dict_to_json(data=recipe, key="crawled_recipe/mange/{num}.json".format(num=recipe_num))


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
