import boto3
import numpy as np

from simulator.price import price
from simulator.quantity import quantify

if __name__ == '__main__':

    """
        This function fetches content from MySQL RDS instance
    """

        # After, it's possible to implement parallel computing.
        x_num = 10000
        count = 0

        # x = np.empty((2, 2))
        x_name = []

        """
            carmelCase
            rawData = {
                            fridge: [],
                            recipes: [],
                        }
        """

        for i in range(x_num):
            raw_data = dict()
            fridge = dict()
            recipes = dict()

        for row in cur:
            item_name, item_freq, price_avg, price_delta, price_d_type = row
            # print(row)
            if count < 3:
                print(row)
                x_quantity = quantify(num=x_num, freq=item_freq, d_type=0)
                print(x_quantity)
                x_price = price(num=x_num, avg=price_avg, delta=price_delta, d_type=price_d_type)

                x_tmp = np.column_stack([x_quantity, x_price])
                x_name.append(item_name)
                print(x_tmp.tolist())
                print(x_name)
                x = np.column_stack([x, x_tmp])
                print(x)
            count += 1

        print(x.shape)

    conn.commit()


    # print(__name__)


