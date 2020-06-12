import pandas as pd
from flask import Flask, request, jsonify

from crawler.core import Crawler, RecipeCrawler


def main():
    app = Flask(__name__)

    @app.route('/crawl_recipe', methods=['GET'])
    def crawl_recipe():
        """
            Crawl -> Map -> Store

            - url: crawl recipe data in url
            해먹남녀: https://haemukja.com/recipes/
            만개의 레시피: http://www.10000recipe.com/recipe/

        :return:
        """
        recipe = RecipeCrawler(url="https://haemukja.com/recipes/").process()
        # TODO: map (recipe to json)
        # TODO: store

        return 'hello bobsim!'

    app.run(host='0.0.0.0', port=9000, debug=True)


if __name__ == '__main__':
    main()
