from sqlalchemy import String
import luigi
from luigi.contrib import sqla
from luigi.mock import MockTarget

class BaseTask(luigi.Task):
    def output(self):
        return MockTarget("BaseTask")

    def run(self):
        out = self.output().open("w")
        TASK_LIST = ["item%d\tproperty%d\n" % (i, i) for i in range(15)]
        for task in TASK_LIST:
            out.write(task)
        out.close()

class SQLATask(sqla.CopyToTable):
    # columns defines the table schema, with each element corresponding
    # to a column in the format (args, kwargs) which will be sent to
    # the sqlalchemy.Column(*args, **kwargs)
    columns = [
        (["item", String(64)], {"primary_key": True}),
        (["property", String(64)], {})
    ]
    connection_string = "sqlite:///my.db"  # in memory SQLite database
    table = "item_property"  # name of the table to store data

    def requires(self):
        return BaseTask()

if __name__ == '__main__':
    task1, task2 = SQLATask(), BaseTask()
    luigi.build([task1, task2], local_scheduler=True)