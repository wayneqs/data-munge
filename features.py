from pymongo import MongoClient, DESCENDING, ASCENDING
import datetime, pandas as pd

class DataPoint:
    def __init__(self, ref_date, date, speed):
        self.ref_date = ref_date
        self.date = date
        self.speed = speed
    def value(self):
        return self.speed
    def fade(self):
        if self.date == None:
            return 0
        else:
            diff = self.ref_date - self.date
            return diff.days

class MA:
    def __init__(self, rm, dates):
        self.ma = None
        self.dydx = None
        if len(dates) >= 1:
            self.ma = rm[dates[-1]]
        if len(dates) >= 2:
            self.dydx = self.ma - rm[dates[-2]]

class SpeedTimeSeries:
    def __init__(self, db):
        self.db = db
    def find_one(self, q, s, ref_date):
        record = self.db.speed_ts.find_one(q, sort=s)
        if record == None:
            return DataPoint(ref_date, None, None)
        else:
            return DataPoint(ref_date, record["date"], record["speed"])
    def last(self, id, date):
        q = {"horse": id, "date":{"$lt":date}}
        s = [("date", DESCENDING)]
        return self.find_one(q, s, date)
    def top(self, id, date):
        q = {"horse": id, "date":{"$lt":date}}
        s = [("speed", DESCENDING)]
        return self.find_one(q, s, date)
    def split_for_timeseries(self, q, s):
        dates, speeds = [], []
        for record in self.db.speed_ts.find(q, sort=s):
            dates.append(record["date"])
            speeds.append(record["speed"])
        return dates, speeds
    def ma(self, id, date, length):
        q = {"horse": id, "date":{"$lt":date}}
        s = [("date", ASCENDING)]
        dates, speeds = self.split_for_timeseries(q, s)
        ts = pd.Series(speeds, dates)
        rm = pd.rolling_mean(ts, length)
        return MA(rm, dates)
    def sma(self, id, date):
        return self.ma(id, date, 3)
    def lma(self, id, date):
        return self.ma(id, date, 6)

class Runner:
    def __init__(self, data):
        self.data = data
    def id(self):
        return self.data["id"]
    def rating(self):
        rating = self.data["rating"]
        if rating == None:
            return 0
        else:
            return rating
    def std_dev(self):
        return self.data["std-dev"]
    def odds(self):
        odds = self.data["odds"]
        if odds == None:
            return 1.0
        else:
            return odds
    def position(self):
        return self.data["position"]
    def race_id(self):
        return self.data["race-id"]

# possibly going to be use for normalisation
class Races:
    def __init__(self):
        self.races = []
    def append(self, race):
        self.races.append(race)

class Race:
    def __init__(self, id, date, race_type, distance):
        self.runners = []
        self.race_id = id
        self.race_date = date
        self.race_type = race_type
        self.race_distance = distance
    def id(self):
        return self.race_id
    def date(self):
        return self.race_date.strftime("%d-%m-%Y")
    def type(self):
        return self.race_type
    def distance(self):
        return self.race_distance
    def add(self, runner):
        self.runners.append(runner)

db = MongoClient().racing_data

def get_rating(runner, d):
    q = {"id": runner["horse"]["id"], "date":{"$lt":d}}
    rating_cursor = db.rating.find(q).sort("date", DESCENDING)
    if rating_cursor.count() >= 1:
        return rating_cursor[0]
    else:
        return {"rating": 0, "std-dev":0}

def get_races(d):
    races = db.cleaned_races.find({"date": d})
    race_list = []
    speed_gateway = SpeedTimeSeries(db)
    for dbRace in races:
        race = Race(dbRace["race-id"], d, dbRace["race-type"], dbRace["distance"])
        for runner in dbRace["runners"]:
            id = runner["horse"]["id"]
            last = speed_gateway.last(id, d)
            top = speed_gateway.top(id, d)
            sma = speed_gateway.sma(id, d)
            lma = speed_gateway.lma(id, d)
            rating = get_rating(runner, d)
            race.add(Runner({
                "id": id,
                "age": runner["age"],
                "weight-total": runner["weight"]["actual"],
                "weight-jockey-allowance": runner["weight"]["jockey-allowance"],
                "weight-overhandicap": runner["weight"]["over-handicap"],
                "position": runner["position"],
                "rating": rating["rating"],
                "odds": runner["horse"]["odds"],
                "std-dev": rating["std-dev"],
                "speed-last": last.value(),
                "speed-last-fade": last.fade(),
                "speed-top": top.value(),
                "speed-top-fade": top.fade(),
                "speed-sma": sma.ma,
                "speed-sma-dydx": sma.dydx,
                "speed-lma": lma.ma,
                "speed-lma-dydx": lma.dydx
                }))
        race_list.append(race)
    return race_list

def decrementing_iter(from_date=None, to_date=None, delta=datetime.timedelta(days=1)):
    from_date = from_date or datetime.datetime.combine(datetime.date.today(), datetime.time())
    while to_date is None or from_date >= to_date:
        yield from_date
        from_date = from_date - delta
    return

#earliest_date = db.cleaned_races.find_one(fields={"date"}, sort=[("date", ASCENDING)])["date"]
earliest_date = datetime.datetime.combine(datetime.date.today(), datetime.time()) - datetime.timedelta(days=2)
races = []
for date in decrementing_iter(to_date=earliest_date):
    races.extend(get_races(date))
print races[0]
