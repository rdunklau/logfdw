from multicorn import ForeignDataWrapper
import re

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

    def execute(self, quals, columns):
        with open(self.log_file, 'r') as f:
            for line in f:
                match = self.line_re.match(line)
                if match:
                    yield match.groups()
