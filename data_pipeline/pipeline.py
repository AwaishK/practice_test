"""This join different action together at one place
"""
import datetime

from data_pipeline.calculate_aggregates import CalculateAggregates
from data_pipeline.process_raw_data import LoadRawData


class Pipeline:
    def __init__(self, dt_from: datetime.datetime, dt_to: datetime.datetime) -> None:
        """
        Execute all action at one place in order
        :param dt_from (pd.Timestamp): datetime from which to load the data
        :param dt_to (pd.Timestamp): datetime up to which to load the data
        """

        self.dt_from = str(dt_from)
        self.dt_to = str(dt_to)
    
    def run(self) -> None:
        with open("pipeline.log") as f:
            f.write(f"running at {datetime.datetime.now()}\n")

        # load raw data
        load_raw_data = LoadRawData(dt_from=self.dt_from, dt_to=self.dt_to)
        load_raw_data.run()

        # calculate aggregates
        aggregates = CalculateAggregates(dt_from=self.dt_from, dt_to=self.dt_to)
        aggregates.run()


def main():
    EVERY_DAY = 18
    dt_to = datetime.datetime.now().replace(hour=18, minute=0, second=0, microsecond=0)
    dt_from = dt_to - datetime.timedelta(days=1)

    ld = Pipeline(dt_from=dt_from, dt_to=dt_to)
    ld.run()



if __name__ == "__main__":
    main()

