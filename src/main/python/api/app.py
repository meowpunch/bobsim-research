from flask import Flask, jsonify, request

from crawler.core import MangeCrawler, HaemukCrawler
from util.logging import init_logger
from util.s3_manager.manage import S3Manager


def main():
    logger = init_logger()

    app = Flask(__name__)
    app.config['JSON_AS_ASCII'] = False
    app.config['JSON_SORT_KEYS'] = False

    # TODO: classify crawl recipe API service
    @app.route('/', methods=['GET'])
    def index():
        return "<h3>crawling service</h3>\
                <strong>mange</strong><br>\
                [GET] : /crawl_recipe/mange <br>\
                [GET] : /recipe/mange<br><br>\
                <strong>haemuk</strong><br>\
                [GET] : /crawl_recipe/haemuk<br>"

    @app.route('/crawl_recipe/mange', methods=['GET'])
    def crawl_mange():
        """
        :return: jsonified recipe
        """
        args = request.args
        try:
            str_num = args["str_num"]
            end_num = args["end_num"]
        except KeyError:
            logger.warning("There is no parameter, 'str_num' or 'end_num'")
            str_num = 6828808
            end_num = 6828811

        logger.info("let's crawl {s} ~ {e} recipes".format(s=str_num, e=end_num))
        result = MangeCrawler(
            base_url="https://www.10000recipe.com/recipe",
            candidate_num=range(int(str_num), int(end_num)),
            field=['title', 'description', 'views', 'time', 'person', 'difficulty',
                   'items', 'steps', 'caution', 'writer', 'comments', 'tag'],
            bucket_name="production-bobsim",
            key="crawled_recipe/mange"
        ).process()
        return jsonify(result)
        # exit_code = MangeCrawler().process()
        # return str(exit_code)

    @app.route('/recipe/mange', methods=['GET'])
    def get_mange_recipes():
        recipes = S3Manager("production-bobsim").fetch_jsons(key="crawled_recipe/mange")
        return jsonify(recipes)

    @app.route('/crawl_recipe/haemuk', methods=['GET'])
    def crawl_haemuk():
        """
        :return: exit code
        """
        exit_code = HaemukCrawler().process()
        # TODO: map (recipe to json)
        # TODO: store

        return str(exit_code)

    app.run(host='0.0.0.0', port=9000, debug=True)


if __name__ == '__main__':
    main()
