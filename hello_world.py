import luigi
import os
from luigi.contrib import sqla
import pandas as pd
from sqlalchemy import String, create_engine
import datetime
from utils import MTimeMixin, TableReplaceMixin

class GenerateWords(luigi.Task):

    def output(self):
        return luigi.LocalTarget('words.csv')

    def run(self):

        # write a dummy list of words to output file
        words = [
                'apple',
                'banana',
                'grapefruit',
                'Donald'
                ]

        df = pd.DataFrame(dict(words=words))
        df.to_csv(self.output().path, index=False)


class CountLetters(MTimeMixin, luigi.Task):

    def requires(self):
        return GenerateWords()

    def output(self):
        return luigi.LocalTarget('letter_counts.csv')

    def run(self):
        df = pd.read_csv(self.input().path)
        df['letter_count'] = df.words.map(len)
        df.to_csv(self.output().path, index=False)


class SQLStore(luigi.Task):
    
    @property
    def update_id(self):
        try:
            mtime = os.path.getmtime(self.input().path)
            return datetime.datetime.fromtimestamp(mtime).strftime("%m/%d/%Y, %H:%M:%S")
        except:
            return datetime.datetime.today().strftime("%m/%d/%Y, %H:%M:%S")

    def requires(self):
        return CountLetters()

    def output(self):
        return sqla.SQLAlchemyTarget(
            connection_string = "sqlite:///my.db",
            update_id = self.update_id,
            target_table = 'letter_count'
            )
    
    def run(self):
        con = create_engine('sqlite:///my.db')
        df = pd.read_csv(self.input().path)
        df.to_sql(name='letter_count', con=con, if_exists='replace')
        
        print(f'####################{self.output().update_id}#####################')

        # Update Marker Table
        self.output().touch() 


class Test(luigi.Task):

    def requires(self):
        return SQLStore()

    def complete(self):
        return False
    
    def output(self):
        pass

    def run(self):
        input = self.input()
        
        print(f'##############{input.target_table}##############')
        print(f'##############{input.engine}##############')
        print(pd.read_sql(input.target_table, input.engine))
# class SQLATask(TableReplaceMixin, sqla.CopyToTable):
#     # https://luigi.readthedocs.io/en/stable/_modules/luigi/contrib/sqla.html
#     # columns defines the table schema, with each element corresponding
#     # to a column in the format (args, kwargs) which will be sent to
#     # the sqlalchemy.Column(*args, **kwargs)
#     date = luigi.DateParameter(default=datetime.date.today())
    
#     columns = [
#         (["words", String()], {"primary_key": True}),
#         (["letter_count", String()], {})

#     ]
#     connection_string = "sqlite:///my.db"  # in memory SQLite database
#     table = "word_count"  # name of the table to store data
#     column_separator = ','

#     def requires(self):
#         return CountLetters()

#     def update_id(self):
#         return self.date

#     def rows(self):
#         """
#         Return/yield tuples or lists corresponding to each row to be inserted.

#         This method can be overridden for custom file types or formats.
#         """
#         with self.input().open('r') as fobj:
#             # Skip Headers
#             next(fobj)
#             # Read the rest of the rows
#             for line in fobj:
#                 yield line.strip("\n").split(self.column_separator)

if __name__ == '__main__':
    luigi.build([Test()], local_scheduler=False)
    # central server @ http://localhost:8082/
