from abc import ABC, abstractmethod

import pandas as pd


class BaseLoaderStrategy(ABC):
    """
    Abstract base class for all domain loading strategies.
    """

    @abstractmethod
    def load_data(self, df: pd.DataFrame, postgres_conn) -> None:
        """
        Load data into the domain.

        :param data: DataFrame containing the data to be loaded.
        """
        pass
