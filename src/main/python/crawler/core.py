from collections import OrderedDict
from urllib.error import HTTPError

from selenium import webdriver
from selenium.common.exceptions import UnexpectedAlertPresentException, NoSuchElementException

from util.logging import init_logger
from util.s3_manager.manage import S3Manager


class RecipeCrawler:
    def __init__(self, base_url, candidate_num, field, bucket_name, key):
        self.logger = init_logger()
        self.s3_manager = S3Manager(bucket_name=bucket_name)
        self.key = key

        self.chrome_path = "C:/chromedriver"
        options = webdriver.ChromeOptions()
        options.add_argument('headless')

        self.driver = webdriver.Chrome(executable_path=self.chrome_path, chrome_options=options)
        self.driver.implicitly_wait(3)

        self.base_url = base_url
        self.candidate_num = candidate_num

        self.field = field

    def process(self):
        """
            1. crawl recipes
            2. filter result
            3. save
            4. quit driver
        :return: recipes: dict
        """
        result = map(lambda n: (n, self.crawl_recipe(recipe_num=n)), self.candidate_num)
        recipes = dict(filter(lambda r: False not in r, result))
        self.save(
            recipes=recipes,
            file_name="{str}-{end}".format(str=self.candidate_num[0], end=self.candidate_num[-1])
        )

        self.logger.info("success to save {n} recipes".format(n=len(recipes)))
        self.driver.quit()
        return recipes

    def crawl_recipe(self, recipe_num):
        """
            1. connection
            2. get recipe:dict
        :return: recipe(Success) or False(Fail)
        """
        try:
            self.connection(recipe_num=recipe_num)
            self.driver.implicitly_wait(3)

            recipe = self.get_recipe()
            self.logger.info(recipe)
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
            self.logger.exception(e, exc_info=False)
            return False

        except NotImplementedError as e:
            self.logger.exception(e, exc_info=True)
            return False

    def connection(self, recipe_num=6847470):
        target_url = "{base_url}/{num}".format(base_url=self.base_url, num=str(recipe_num))
        self.driver.get(target_url)
        self.logger.debug("success to connect with '{url}'".format(url=target_url))

    def save(self, recipes: dict, file_name):
        self.s3_manager.save_dict_to_json(
            data=recipes,
            key="{prefix}/{key}.json".format(prefix=self.key, key=file_name)
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
            self.logger.debug("No Element about '{k}'".format(k=key), exc_info=False)
            if key == "title":
                raise UnexpectedAlertPresentException
            return key, None
        except KeyError:
            raise NotImplementedError

    def select_element(self, key):
        pass


class MangeCrawler(RecipeCrawler):
    def __init__(self, base_url="https://www.10000recipe.com/recipe", candidate_num=range(6828809, 6828811), field=None,
                 bucket_name="production-bobsim", key="crawled_recipe/mange"):
        """
            https://www.10000recipe.com/recipe/
            recipe_num: about 6828805 ~ 6935000

            record:
                            total_recipe
                15.06.2020: 138,873
        """
        if field is None:
            field = ['title', 'description', 'views', 'time', 'person', 'difficulty',
                     'items', 'steps', 'caution', 'writer', 'comments', 'tag']

        super().__init__(
            base_url=base_url,
            candidate_num=candidate_num,
            field=field,
            bucket_name=bucket_name,
            key=key
        )

    def select_element(self, key):
        return {
            "title": lambda d: d.find_element_by_tag_name("h3").text,
            "description": lambda d: d.find_element_by_class_name("view2_summary_in").text,
            "views": lambda d: d.find_element_by_class_name("hit").text,
            "scrap": lambda d: d.find_element_by_xpath('//*[@id="contents_area"]/div[2]/div[3]/a[1]/span').text,
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


class HaemukCrawler(RecipeCrawler):
    def __init__(self, base_url="https://www.haemukja.com/recipes", candidate_num=range(5000, 5001), field=None,
                 bucket_name="production_bobsim", key="crawled_recipe/haemuk"):
        """
            https://www.haemukja.com/recipes
            recipe_num: about ? ~ ?

            record:
                            total_recipe
                15.06.2020: 5,386
        """
        if field is None:
            field = []

        super().__init__(
            base_url=base_url,
            candidate_num=candidate_num,
            field=field,
            bucket_name=bucket_name,
            key=key
        )

    def select_element(self, key):
        return {
            "title": lambda d: d.find_element_by_class_name("top").find_element_by_tag_name("h1").text,
            "calories": lambda d: d.find_element_by_class_name("nutrition").text.split("\n"),
        }[key](self.driver)
