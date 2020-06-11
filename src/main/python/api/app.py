import pandas as pd
from flask import Flask, request, jsonify


def main():
    app = Flask(__name__)

    @app.route('/crawl_recipe', methods=['GET'])
    def crawl_recipe():
        return 'hello bobsim!'

    app.run(host='0.0.0.0', port=9000, debug=True)


if __name__ == '__main__':
    main()
