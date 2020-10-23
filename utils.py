
import time
import os
from pathlib import Path
from sqlalchemy import MetaData, Table, Column
from sqlalchemy.ext.declarative import declarative_base

class MTimeMixin:
    """
        Mixin that flags a task as incomplete if any requirement
        is incomplete or has been updated more recently than this task
        This is based on http://stackoverflow.com/a/29304506, but extends
        it to support multiple input / output dependencies.
    """

    def complete(self):
        def to_list(obj):
            if type(obj) in (type(()), type([])):
                return obj
            else:
                return [obj]

        def mtime(path):
            return time.ctime(os.path.getmtime(path))

        if not all(os.path.exists(out.path) for out in to_list(self.output())):
            return False

        self_mtime = min(mtime(out.path) for out in to_list(self.output()))

        # the below assumes a list of requirements, each with a list of outputs. YMMV
        for el in to_list(self.requires()):
            if not el.complete():
                # Fixes Windows FileExistsError
                if os.path.exists(self.output().path):
                    os.remove(self.output().path)
                return False
            for output in to_list(el.output()):
                if mtime(output.path) > self_mtime:
                    # Fixes Windows FileExistsError
                    os.remove(self.output().path)
                    
                    return False

        return True


class DisplayablePath(object):
    # https://stackoverflow.com/questions/9727673/list-directory-tree-structure-in-python
    
    display_filename_prefix_middle = '├──'
    display_filename_prefix_last = '└──'
    display_parent_prefix_middle = '    '
    display_parent_prefix_last = '│   '

    def __init__(self, path, parent_path, is_last):
        self.path = Path(str(path))
        self.parent = parent_path
        self.is_last = is_last
        if self.parent:
            self.depth = self.parent.depth + 1
        else:
            self.depth = 0

    @property
    def displayname(self):
        if self.path.is_dir():
            return self.path.name + '/'
        return self.path.name

    @classmethod
    def make_tree(cls, root, parent=None, is_last=False, criteria=None):
        root = Path(str(root))
        criteria = criteria or cls._default_criteria

        displayable_root = cls(root, parent, is_last)
        yield displayable_root

        children = sorted(list(path
                               for path in root.iterdir()
                               if criteria(path)),
                          key=lambda s: str(s).lower())
        count = 1
        for path in children:
            is_last = count == len(children)
            if path.is_dir():
                yield from cls.make_tree(path,
                                         parent=displayable_root,
                                         is_last=is_last,
                                         criteria=criteria)
            else:
                yield cls(path, displayable_root, is_last)
            count += 1

    @classmethod
    def _default_criteria(cls, path):
        return True

    @property
    def displayname(self):
        if self.path.is_dir():
            return self.path.name + '/'
        return self.path.name

    def displayable(self):
        if self.parent is None:
            return self.displayname

        _filename_prefix = (self.display_filename_prefix_last
                            if self.is_last
                            else self.display_filename_prefix_middle)

        parts = ['{!s} {!s}'.format(_filename_prefix,
                                    self.displayname)]

        parent = self.parent
        while parent and parent.parent is not None:
            parts.append(self.display_parent_prefix_middle
                         if parent.is_last
                         else self.display_parent_prefix_last)
            parent = parent.parent

        return ''.join(reversed(parts))


class TableReplaceMixin:

    def create_table(self, engine):
        """
        Override to provide code for creating the target table.

        By default it will be created using types specified in columns.
        If the table exists, then it binds to the existing table.

        If overridden, use the provided connection object for setting up the table in order to
        create the table and insert data using the same transaction.
        :param engine: The sqlalchemy engine instance
        :type engine: object
        """
        def construct_sqla_columns(columns):
            retval = [Column(*c[0], **c[1]) for c in columns]
            return retval

        needs_setup = (len(self.columns) == 0) or (False in [len(c) == 2 for c in self.columns]) if not self.reflect else False
        if needs_setup:
            # only names of columns specified, no types
            raise NotImplementedError("create_table() not implemented for %r and columns types not specified" % self.table)
        else:
            # if columns is specified as (name, type) tuples
            with engine.begin() as con:
                 
                if self.schema:
                    metadata = MetaData(schema=self.schema)
                else:
                    metadata = MetaData()

                try:
                    # Check to see if table exists and drop it then recreate table
                    # https://stackoverflow.com/questions/35918605/how-to-delete-a-table-in-sqlalchemy

                    existing_table = MetaData(con, reflect=True).tables.get(self.table)
                    if existing_table is not None:
                        base = declarative_base()
                        base.metadata.drop_all(engine, [existing_table], checkfirst=True)

                    sqla_columns = construct_sqla_columns(self.columns)
                    self.table_bound = Table(self.table, metadata, *sqla_columns)
                    metadata.create_all(engine)

                except Exception as e:
                    self._logger.exception(self.table + str(e))        