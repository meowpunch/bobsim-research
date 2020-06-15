import pandas as pd
from flask import Flask, request, jsonify

from crawler.core import RecipeCrawler, MangeCrawler, HaemukCrawler


def main():
    app = Flask(__name__)

    @app.route('/crawl_recipe/mange', methods=['GET'])
    def mange_recipe():
        """
            Crawl -> Map -> Store
        :return: list of exit_code about each recipe
        """
        exit_code = MangeCrawler().process()
        # TODO: map (recipe to json)
        # TODO: store

        return str(exit_code)

    @app.route('/crawl_recipe/haemuk', methods=['GET'])
    def haemuk_recipe():
        """
            Crawl -> Map -> Store
        :return:
        """
        exit_code = HaemukCrawler().process()
        # TODO: map (recipe to json)
        # TODO: store

        return str(exit_code)

    app.run(host='0.0.0.0', port=9000, debug=True)


if __name__ == '__main__':
    main()
