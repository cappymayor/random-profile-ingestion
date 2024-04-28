import pandas as pd
from faker import Faker
import logging

# setting logging to enable us to debug when code fails
logging.getLogger().setLevel(20)


def random_profile_data_generator(total_records: int):
    """
       Function that generates random profile for
       different individuals and build a pandas dataframe
       based on the number of profile specified.

       params:
            total_records: Total number of random profiles
            to generate.
    """

    sample = Faker()
    logging.info("finished faker module instantiation")

    dataframe = pd.DataFrame(
        [sample.profile() for profile in range(total_records)])
    logging.info(f"Dataframe created with {dataframe.shape[1]} records")

    return dataframe.shape
