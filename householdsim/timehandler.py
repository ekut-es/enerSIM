from datetime import datetime, timezone

# DATE_FORMAT = 'YYYY-MM-DDTHH:mm:ssZ'


class TimeHandler:

    @staticmethod
    def utc_to_timestamp(utc_timestamp):
        date = utc_timestamp.split("T")[0]
        year = date.split("-")[0]
        month = date.split("-")[1]
        day = date.split("-")[2]
        time = utc_timestamp.split("T")[1][:-1]
        hour = time.split(":")[0]
        minute = time.split(":")[1]
        second = time.split(":")[2]
        real_utc = datetime(int(year), int(month), int(day), int(hour), int(minute), int(second), tzinfo=timezone.utc)
        timestamp = datetime.timestamp(real_utc)
        return timestamp

    @staticmethod
    def timestamp_to_utc(timestamp):
        utc_timestamp = datetime.utcfromtimestamp(timestamp).strftime('%Y-%m-%dT%H:%M:%SZ')
        return utc_timestamp

    @staticmethod
    def add_minutes_to_timestamp(timestamp, minutes):
        return timestamp + (minutes * 60)

    @staticmethod
    def datetime_to_utc(time):
        month = TimeHandler.add_preceding_zero(str(time.month))
        day = TimeHandler.add_preceding_zero(str(time.day))
        hour = TimeHandler.add_preceding_zero(str(time.hour))
        minute = TimeHandler.add_preceding_zero(str(time.minute))
        second = TimeHandler.add_preceding_zero(str(time.second))
        utc_time = str(time.year) + "-" + month + "-" + day + "T" + hour + ":" + minute + ":" + second + "Z"
        return utc_time

    @staticmethod
    def add_preceding_zero(time_string):
        if len(time_string) == 1:
            time_string = '0' + time_string
        return time_string

    @staticmethod
    def normal_utc_to_datetime(utc_time):
        date = utc_time.split(" ")[0]
        year = date.split("-")[0]
        month = date.split("-")[1]
        day = date.split("-")[2]
        time = utc_time.split(" ")[1]
        hour = time.split(":")[0]
        minute = time.split(":")[1]
        second = time.split(":")[2]
        real_utc = datetime(int(year), int(month), int(day), int(hour), int(minute), int(second), tzinfo=timezone.utc)
        return real_utc

    @staticmethod
    def add_minutes(time, minutes):
        timestamp = datetime.timestamp(time)
        timestamp = TimeHandler.add_minutes_to_timestamp(timestamp, minutes)
        time = datetime.fromtimestamp(timestamp, tz=timezone.utc)
        return time

    def get_time_duration(self, start, end):
        start = self.utc_to_timestamp(start)
        end = self.utc_to_timestamp(end)
        duration_in_minutes = abs(start - end) / 60
        return duration_in_minutes

    # is time1 later or equal than time2
    def is_later_or_equal(self, utctime1, utctime2):
        timestamp1 = self.utc_to_timestamp(utctime1)
        timestamp2 = self.utc_to_timestamp(utctime2)
        return timestamp1 >= timestamp2


def main():
    time = datetime(2016, 4, 22, 15, 0, 0, tzinfo=timezone.utc)
    time = TimeHandler.add_minutes(time, 15)
    print(time)


if __name__ == '__main__':
    main()