import io
import urllib.request
from collections import OrderedDict
from functools import reduce
from urllib.error import HTTPError

import boto3
from selenium import webdriver
from selenium.common.exceptions import UnexpectedAlertPresentException, NoSuchElementException

from utils.function import take, add
from utils.logging import init_logger
from utils.s3_manager.manage import S3Manager
from utils.string import get_digits_from_str, get_float_from_str


class RecipeCrawler:
    def __init__(self, base_url, candidate_num, field, bucket_name, key):
        self.logger = init_logger()

        self.bucket_name = bucket_name
        self.s3_manager = S3Manager(bucket_name=self.bucket_name)
        self.prefix = key

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
        self.save_recipes_to_s3(
            recipes=recipes,
            file_name="{str}-{end}".format(str=self.candidate_num[0], end=self.candidate_num[-1])
        )

        self.logger.info("success to save {n} recipes".format(n=len(recipes)))
        self.driver.quit()
        return recipes

    def crawl_recipe(self, recipe_num):
        """
            1. connection
            2. get recipe -> dict
        :return: recipe(Success) or False(Fail)
        """
        try:
            self.connection(recipe_id=recipe_num)
            self.driver.implicitly_wait(3)

            recipe = self.get_recipe()
            if self.save_image_to_s3(recipe_id=recipe_num):
                recipe["img_url"] = self.get_s3_image_url(recipe_id=recipe_num)
            else:
                raise ConnectionError

            # TODO: logging error (UnicodeEncodeError)
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

    def connection(self, recipe_id=6847470) -> None:
        target_url = "{base_url}/{num}".format(base_url=self.base_url, num=str(recipe_id))
        self.driver.get(target_url)
        self.logger.debug("success to connect with '{url}'".format(url=target_url))

    def save_recipes_to_s3(self, recipes: dict, file_name) -> bool:
        return self.s3_manager.save_dict_to_json(
            data=recipes,
            key="{prefix}/{name}.json".format(prefix=self.prefix, name=file_name)
        )

    def get_recipe(self) -> OrderedDict:
        """
        :return: recipe: dict
        """
        return OrderedDict(map(self.make_tuple, self.field))

    def make_tuple(self, key) -> tuple:
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
        return {
            "title": self.get_title,
            "time": self.get_time,
            "person": self.get_person,
            "items": self.get_items,
            "tags": self.get_tags,
        }[key]()

    def save_image_to_s3(self, recipe_id) -> bool:
        url = self.get_img_url()
        with urllib.request.urlopen(url) as url:
            img = io.BytesIO(url.read())
        return self.s3_manager.save_img(
            data=img, key="{prefix}/images/{recipe_id}.jpg".format(prefix=self.prefix, recipe_id=recipe_id),
            kwargs={"ACL": 'public-read', 'ContentType': 'image/jpg'}
        )

    def get_s3_image_url(self, recipe_id) -> str:
        # get s3 url of main img
        return "https:/{bucket}.s3.{region}.amazonaws.com/{key}".format(
            bucket=self.bucket_name,
            region=boto3.client('s3').get_bucket_location(Bucket=self.bucket_name)['LocationConstraint'],
            key="{prefix}/images/{recipe_id}.jpg".format(prefix=self.prefix, recipe_id=recipe_id)
        )

    def get_img_url(self) -> str:
        pass

    def get_title(self) -> str:
        pass

    def get_time(self) -> int:
        """
            unit: minute
        """
        pass

    def get_person(self) -> int:
        """
            unit: person
        """
        pass

    def get_items(self) -> dict:
        """
            { name: amount ... }
        """
        pass

    def get_tags(self) -> list:
        """
            max 3 tags
        """
        pass


class MangaeCrawler(RecipeCrawler):
    def __init__(self, base_url="https://www.10000recipe.com/recipe", candidate_num=range(6828809, 6828811), field=None,
                 bucket_name="production-bobsim", key="crawled_recipe/mangae"):
        """
            https://www.10000recipe.com/recipe/
            recipe_num: about 6828805 ~ 6935000

            record:
                            total_recipe
                15.06.2020: 138,873
        """
        if field is None:
            field = ['title', 'time', 'person', 'items', 'tags']

        super().__init__(
            base_url=base_url,
            candidate_num=candidate_num,
            field=field,
            bucket_name=bucket_name,
            key=key
        )

    def get_img_url(self) -> str:
        return self.driver.find_element_by_xpath('//*[@id="main_thumbs"]').get_attribute('src')

    def get_title(self) -> str:
        title = self.driver.find_element_by_xpath('//*[@id="contents_area"]/div[2]/h3')
        return title.text

    def get_time(self) -> int:
        time = self.driver.find_element_by_xpath('//*[@id="contents_area"]/div[2]/div[2]/span[2]').text.split(" ")[0]
        if "분" in time:
            return int(get_digits_from_str(string=time))
        elif "시간" in time:
            return int(get_digits_from_str(string=time)) * 60
        else:
            raise ValueError

    def get_person(self) -> int:
        person = self.driver.find_element_by_class_name("view2_summary_info1").text
        return int(get_digits_from_str(string=person))

    def get_items(self) -> dict:
        items = self.driver.find_elements_by_xpath('//*[@id="divConfirmedMaterialArea"]/ul//li')

        def get_amount(item) -> tuple:
            try:
                return item.text.split('\n')[0], get_float_from_str(item.find_element_by_tag_name('span').text)
            except NoSuchElementException:
                raise ValueError

        return dict(map(get_amount, items))

    def get_tags(self) -> list:
        tags = self.driver.find_elements_by_xpath('//*[@id="contents_area"]/div[32]/div/a')
        return list(take(length=3, iterator=map(lambda tag: tag.text.replace("#", ""), tags)))


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
            field = ['title', 'time', 'person', 'items', 'tags']

        super().__init__(
            base_url=base_url,
            candidate_num=candidate_num,
            field=field,
            bucket_name=bucket_name,
            key=key
        )

    def get_title(self) -> str:
        return self.driver.find_element_by_xpath('//*[@id="container"]/div[2]/div/div[1]/section[1]/div/div[1]/h1').text

    def get_items(self) -> dict:
        items = self.driver.find_elements_by_xpath('//*[@id="container"]/div[2]/div/div[1]/section[1]/div/div[3]/ul/li')

        def get_amount(item) -> tuple:
            try:
                name, amount = item.find_element_by_tag_name("span").text, item.find_element_by_tag_name("em").text
                return name, get_float_from_str(amount)
            except NoSuchElementException:
                raise ValueError

        return dict(filter(lambda x: x[1] != "", map(get_amount, items)))

    def get_tags(self) -> list:
        tags = self.driver.find_elements_by_class_name('//*[@id="modal-content"]/div/div[1]/section[2]/div[3]/a')
        return list(take(length=3, iterator=map(lambda tag: tag.text, tags)))

    def get_time(self) -> int:
        time = self.driver.find_element_by_xpath(
            '//*[@id="container"]/div[2]/div/div[1]/section[1]/div/div[1]/dl/dd[1]').text
        return int(get_digits_from_str(string=time))

    def get_person(self) -> int:
        person = self.driver.find_element_by_class_name("dropdown").text
        return int(reduce(add, filter(str.isdigit, person)))

    def get_img_url(self) -> str:
        return self.driver.find_element_by_xpath('//*[@id="slider"]/div/ul/li[1]/img').get_attribute("src")
