from flask import Flask, jsonify

from crawler.core import MangeCrawler, HaemukCrawler
from util.s3_manager.manage import S3Manager


def main():
    app = Flask(__name__)
    app.config['JSON_AS_ASCII'] = False
    app.config['JSON_SORT_KEYS'] = False

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
        result = MangeCrawler(
            base_url="https://www.10000recipe.com/recipe",
            candidate_num=range(6828808, 6828811),
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
