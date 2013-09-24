from multicorn import ForeignDataWrapper
import re
from datetime import datetime

class LogFDW(ForeignDataWrapper):

    def __init__(self, fdw_options, fdw_columns):
        super(LogFDW, self).__init__(fdw_options, fdw_columns)
        self.log_file = fdw_options.get('log_file', None)
        if self.log_file is None:
            raise ValueError('The log_file option is mandatory')
        # Default to matching the whole line.
        self.line_re = re.compile(fdw_options.get('line_pattern', "(.*)"))
        if len(fdw_columns) != self.line_re.groups:
            raise ValueError('The table should have as much columns as '
                             'there are groups in the pattern')
        self.timestamp_group = None
        self.timestamp_field = None
        self.timestamp_pattern = None
        for idx, col in enumerate(fdw_columns.values()):
            if col .options.get('is_timestamp', False):
                self.timestamp_group = idx
                self.timestamp_field = col.column_name
                self.timestamp_pattern = col.options.get('pattern',
                                                         '%Y-%m-%d %H:%M:%S')

    def execute(self, quals, columns):
        max_date = None
        if self.timestamp_group is not None:
            for qual in quals:
                if (qual.operator == '<' and
                    qual.field_name == self.timestamp_field):
                        max_date = qual.value


        with open(self.log_file, 'r') as f:
            for line in f:
                match = self.line_re.match(line)
                if match:
                    groups = match.groups()
                    if max_date:
                        log_date_str = groups[self.timestamp_group]
                        log_date = datetime.strptime(log_date_str, self.timestamp_pattern)
                        if not log_date < max_date:
                            # Stop iteration here
                            return
                    yield match.groups()
