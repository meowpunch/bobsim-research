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
                [GET] : /crawl_recipe/haemuk<br>\
                [GET] : /recipe/haemuk<br><br>"

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
            str_num, end_num = 6828808, 6828811

        logger.info("let's crawl {str} ~ {end} {source} recipes".format(str=str_num, end=end_num, source=source))
        field = ['title', 'items', "time", "person", "tags", "img_url"]
        if source == "mange":
            result = MangeCrawler(
                base_url="https://www.10000recipe.com/recipe",
                candidate_num=range(int(str_num), int(end_num)),
                field=field,
                bucket_name="production-bobsim",
                key="crawled_recipe/{s}".format(s=source)
            ).process()
        elif source == "haemuk":
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
        # exit_code = MangeCrawler().process()
        # return str(exit_code)

    @app.route('/recipe/<source>', methods=['GET'])
    def get_recipes(source):
        recipes = S3Manager("production-bobsim").fetch_dict_from_json(key="crawled_recipe/{s}".format(s=source))
        return jsonify(recipes)

    app.run(host='0.0.0.0', port=9000, debug=True)


if __name__ == '__main__':
    main()
