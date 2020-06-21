from flask import Flask, jsonify, request

from crawler.core import MangaeCrawler, HaemukCrawler
from utils.logging import init_logger
from utils.s3_manager.manage import S3Manager


def main():
    logger = init_logger()

    app = Flask(__name__)
    app.config['JSON_AS_ASCII'] = False
    app.config['JSON_SORT_KEYS'] = False

    # TODO: classify crawl recipe API service
    @app.route('/', methods=['GET'])
    def index():
        return "<h3>crawling service</h3>\
                <strong>Mangae</strong><br>\
                [GET] : /crawl_recipe/M?str_num=6932924&end_num=6932926<br>\
                [GET] : /recipe/M<br><br>\
                <strong>Haemuk</strong><br>\
                [GET] : /crawl_recipe/H?str_num=5004&end_num=5005<br>\
                [GET] : /recipe/H<br><br>"

    @app.route('/crawl_recipe/<source>', methods=['GET'])
    def crawl_recipe(source):
        """
        :return: jsonified recipe
        """
        args = request.args
        try:
            str_num, end_num = args["str_num"], args["end_num"]
        except KeyError:
            logger.warning("There is no parameter, 'str_num' or 'end_num'")
            str_num, end_num = 6934386, 6934390

        logger.info("let's crawl {str} ~ {end} {source} recipes".format(str=str_num, end=end_num, source=source))
        field = ['title', 'items', "duration", "tags"]

        if source == "M":
            result = MangaeCrawler(
                base_url="https://www.10000recipe.com/recipe",
                candidate_num=range(int(str_num), int(end_num)),
                field=field,
                bucket_name="production-bobsim",
                key="crawled_recipe/{s}".format(s=source)
            ).process()
        elif source == "H":
            result = HaemukCrawler(
                base_url="https://www.haemukja.com/recipes",
                candidate_num=range(int(str_num), int(end_num)),
                field=field,
                bucket_name="production-bobsim",
                key="crawled_recipe/{s}".format(s=source)
            ).process()
        else:
            raise NotImplementedError

        return jsonify(result)

    @app.route('/recipe/<source>', methods=['GET'])
    def get_recipes(source):
        recipes = S3Manager("production-bobsim").fetch_dict_from_json(key="crawled_recipe/{s}".format(s=source))
        if recipes is None:
            return 'there is no recipe'
        return jsonify(recipes)

    app.run(host='0.0.0.0', port=9000, debug=True)


if __name__ == '__main__':
    main()
