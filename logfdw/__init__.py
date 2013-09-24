from multicorn import ForeignDataWrapper

class LogFDW(ForeignDataWrapper):

    def execute(self, quals, columns):
        pass
