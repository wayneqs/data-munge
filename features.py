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
    def speed(self, id, date, category):
        q = {"horse": id, "date":{"$lte":date}, "distance-category":category}
        s = [("date", DESCENDING)]
        return self.find_one(q, s, date)
    def diff(self, id, date, category):
        q = {"horse": id, "date":{"$lt":date}, "distance-category":category}
        s = [("date", DESCENDING)]
        results = self.db.find(q, sort=s).limit(2)
        return results[0]["speed"] - results[1]["speed"] if results.count() == 2 else 0
    def last(self, id, date, category):
        q = {"horse": id, "date":{"$lt":date}, "distance-category":category}
        s = [("date", DESCENDING)]
        return self.find_one(q, s, date)
    def top(self, id, date, category):
        q = {"horse": id, "date":{"$lt":date}, "distance-category":category}
        s = [("speed", DESCENDING)]
        return self.find_one(q, s, date)
    def split_for_timeseries(self, q, s):
        dates, speeds = [], []
        for record in self.db.speed_ts.find(q, sort=s):
            dates.append(record["date"])
            speeds.append(record["speed"])
        return dates, speeds
    def ma(self, id, date, length, category):
        q = {"horse": id, "date":{"$lt":date}, "distance-category":category}
        s = [("date", ASCENDING)]
        dates, speeds = self.split_for_timeseries(q, s)
        ts = pd.Series(speeds, dates)
        rm = pd.rolling_mean(ts, length)
        return MA(rm, dates)
    def sma(self, id, date, category):
        return self.ma(id, date, 3, category)
    def lma(self, id, date, category):
        return self.ma(id, date, 6, category)

class Race:
    def __init__(self, data, d):
        self.runners = []
        self.data = data
        self.race_date = d
    def is_short(self):
        return distance_category == "short"
    def is_medium(self):
        return distance_category == "medium"
    def is_long(self):
        return distance_category == "long"
    def id(self):
        return self.data["race-id"]
    def date(self):
        return self.race_date.strftime("%d-%m-%Y")
    def type(self):
        return self.data["race-type"]
    def distance(self):
        return self.data["distance"]
    def distance_category(self):
        return self.data["distance-category"]
    def add(self, runner):
        self.runners.append(runner)
    def dump(self):
        for r in self.runners:
            print "%s\t%s\t%s\t%s\t%s\t%s" % (self.id(), self.date(), self.type(), self.distance(), self.distance_category(), r.to_s())

class Runner:
    def __init__(self, data):
        self.data = data
    def id(self):
        return self.data["id"]
    def odds(self):
        odds = self.data["odds"]
        return 1.0 if odds == None else odds
    def rating(self):
        return self.data["rating"]
    def std_dev(self):
        return self.data["std-dev"]
    def position(self):
        return self.data["position"]
    def race_id(self):
        return self.data["race-id"]
    def speed(self):
        return self.data["speed"]
    def speed_last(self):
        return self.data["speed-last"]
    def speed_last_fade(self):
        return self.data["speed-last-fade"]
    def speed_top(self):
        return self.data["speed-top"]
    def speed_top_fade(self):
        return self.data["speed-top-fade"]
    def speed_sma(self):
        return self.data["speed-sma"]
    def speed_sma_dydx(self):
        return self.data["speed-sma-dydx"]
    def speed_lma(self):
        return self.data["speed-lma"]
    def speed_lma_dydx(self):
        return self.data["speed-lma-dydx"]
    def is_juvi(self):
        return 1 if age() < 4 else 0
    def age(self):
        return self.data["age"]
    def weight_total(self):
        return self.data["weight-total"]
    def weight_jockey_allowance(self):
        return self.data["weight-jockey-allowance"]
    def weight_overhandicap(self):
        return self.data["weight-overhandicap"]
    def to_s(self):
        return "%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s" % (self.odds(), self.rating(), self.std_dev(), self.position(), 
                            self.speed_last(), self.speed_last_fade(), self.speed_top(), self.speed_top_fade(), self.speed_sma(),
                            self.speed_sma_dydx(), self.speed_lma(), self.speed_lma_dydx(), self.age(), self.weight_total(), self.weight_jockey_allowance(),
                            self.weight_overhandicap())

class Normalizer:
    def __init__(self):
        self.ra, self.rb, self.mins, self.maxs = (1.0, -1.0, {}, {})
    def capture(self, values):
        key, value = values
        if key not in self.mins:
            self.mins[key] = value
        if key not in self.maxs:
            self.mins[key] = value
        if self.mins[key] > value:
            self.mins[key] = value
        if self.maxs[key] < value:
            self.maxs[key] = value
    def n(self, key, p):
        return (float(((ra-rb) * (p - a))) / (b - a)) + rb

class Races:
    def __init__(self):
        self.races = []
        self.nr = Normalizer()
    def extend(self, race):
        self.races.extend(race)
    def initialize_normalizer(self):
        for race in self.races:
            nr.capture(("distance", race.distance()))
            for runner in race.runners:
                nr.capture(("rating", runner.rating()))
                nr.capture(("rating-std-dev", runner.std_dev()))
                nr.capture(("speed-last", race.speed_last()))
                nr.capture(("speed-last-fade", race.speed_last_fade()))
                nr.capture(("speed-top", race.speed_top()))
                nr.capture(("speed-top-fade", race.speed_top_fade()))
                nr.capture(("speed-sma", race.speed_sma()))
                nr.capture(("speed-sma-dydx", race.speed_sma_dydx()))
                nr.capture(("speed-lma", race.speed_lma()))
                nr.capture(("speed-lma-dydx", race.speed_lma_dydx()))
                nr.capture(("age", race.age()))
                nr.capture(("weight-total", race.weight_total()))
                nr.capture(("weight-diff", race.weight_diff()))
                nr.capture(("weight-sma", race.weight_sma()))
                nr.capture(("weight-jockey-allowance", race.weight_jockey_allowance()))
                nr.capture(("weight-overhandicap", race.weight_overhandicap()))
    def write(self):
        initialize_normalizer()
        for race in self.races:
            for runner in race.runners:
                print "%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s" % (race.type(),
                       normalizer.n("distance", race.distance()), race.is_short(), race.is_medium(), race.is_long(),
                       normalizer.n("rating", runner.rating()), normalizer.n("rating-std-dev", runner.rating_std_dev()),
                       runner.position(), runner.speed(), normalizer.n("speed-last", runner.speed_last()),
                       normalizer.n("speed-last-fade", runner.speed_last_fade()), normalizer.n("speed-top", runner.speed_top()),
                       normalizer.n("speed-top-fade", runner.speed_top_fade()), normalizer.n("speed-sma", runner.speed_sma()),
                       normalizer.n("speed-sma-dydx", runner.speed_sma_dydx()), normalizer.n("speed-lma", runner.speed_lma()),
                       normalizer.n("speed-lma-dydx", runner.speed_lma_dydx()), normalizer.n("age", runner.age()),
                       runner.is_juvi(), normalizer.n("weight-total", runner.weight_total()),
                       normalizer.n("weight-sma", runner.weight_sma()),
                       normalizer.n("weight-jockey-allowance", runner.weight_jockey_allowance()),
                       normalizer.n("weight-overhandicap", runner.weight_overhandicap()))

db = MongoClient().racing_data

def get_rating(runner, date, distance_category, race_type):
    q = {"id": runner["horse"]["id"], "date":{"$lt":date}, "distance-category": distance_category, "race-type": race_type}
    rating_cursor = db.rating.find(q).sort("date", DESCENDING)
    if rating_cursor.count() >= 1:
        return rating_cursor[0]
    else:
        return {"rating": 0, "std-dev":0}

def get_races(d):
    foundRaces = db.cleaned_races.find({"date": d})
    race_list = []
    speed_gateway = SpeedTimeSeries(db)
    for foundRace in foundRaces:
        race = Race(foundRace, d)
        for runner in foundRace["runners"]:
            id = runner["horse"]["id"]
            speed = speed_gateway.speed(id, d, race.distance_category())
            delta = speed_gateway.diff(id, d, race.distance_category())
            last = speed_gateway.last(id, d, race.distance_category())
            top = speed_gateway.top(id, d, race.distance_category())
            sma = speed_gateway.sma(id, d, race.distance_category())
            lma = speed_gateway.lma(id, d, race.distance_category())
            rating = get_rating(runner, d, race.distance_category(), race.type())
            race.add(Runner({
                "id": id,
                "age": runner["age"],
                "weight-total": runner["weight"]["actual"],
                "weight-jockey-allowance": runner["weight"]["jockey-allowance"],
                "weight-overhandicap": runner["weight"]["over-handicap"],
                "position": runner["position"],
                "rating": rating["rating"],
                "std-dev": rating["std-dev"],
                "odds": runner["horse"]["odds"],
                "speed": speed.value(),
                "speed-diff": delta,
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

class Scorer:
    def __init__(self, races):
        self.races = races
    def all_are_rated(self, race):
        for runner in race.runners:
            if runner.rating() == None or runner.rating() == 0:
                return False
        return True
    def all_have_odds(self, race):
        for runner in race.runners:
            if runner.odds == None or runner.odds() == 0:
                return False
        return True
    def highest_rated(self, race):
        highest = race.runners[0]
        for x in xrange(1, len(race.runners)):
            if highest.rating() < race.runners[x].rating():
                highest = race.runners[x]
        return highest
    def qualifying_races(self):
        qualifying_races = []
        for race in self.races:
            if self.all_are_rated(race) and self.all_have_odds(race):
                qualifying_races.append(race)
        return qualifying_races
    def winner(self, race):
        return race.runners[0]
    def winner_predicted(self, race):
        return self.winner(race).id() == self.highest_rated(race).id()
    def calculate_return(self, stake):
        total = 0.0
        for race in self.qualifying_races():
            if self.winner_predicted(race):
                total += self.winner(race).odds() * stake
            total -= stake
        return total
    def winner_count(self):
        count = 0
        for race in self.qualifying_races():
            if self.winner_predicted(race):
                count += 1
        return count
    def total_count(self):
        return len(self.qualifying_races())
    def winning_races(self):
        result = []
        for race in self.qualifying_races():
            if self.winner_predicted(race):
                result.append(race)
        return result
    def losing_races(self):
        result = []
        for race in self.qualifying_races():
            if not self.winner_predicted(race):
                result.append(race)
        return result

#earliest_date = db.cleaned_races.find_one(fields={"date"}, sort=[("date", ASCENDING)])["date"]
earliest_date = datetime.datetime.combine(datetime.date.today(), datetime.time()) - datetime.timedelta(days=20)
races = Races()
for date in decrementing_iter(to_date=earliest_date):
    races.extend(get_races(date))

scorer = Scorer(races)
print "%s winners out of %s or %s percent" % (scorer.winner_count(), scorer.total_count(), float(scorer.winner_count()) / scorer.total_count() * 100)
print "Profit of %s pounds from a stake of %s pounds per race" % (scorer.calculate_return(1.0), 1.0)
print ""
current_date = datetime.datetime.now()
for race in scorer.winning_races():
    print ""
    print "WINNER!"
    print "RaceID: %s, Type: %s" % (race.id(), race.type())
    print "RaceID\tRaceDate\tRaceType\tDistance\tDCat\tOdds\tRating\tStdDev\tPos\tSpeedL\tSpeedLF\tSpeedT\tSpeedTF\tSpeedSMA\tSpeedSMAD\tSpeedLMA\tSpeedLMAD\tAge\tWeightT\tWeightJA\tWeightOH"
    race.dump()
    #for runner in race.runners:
        #print "id=%-8s position=%-2s rating=%-15s std-dev=%-15s odds=%-5s heat=%s" % (runner.id(), runner.position(), runner.rating(), runner.std_dev(), runner.odds(), runner.rating()/runner.odds())
    print ""
for race in scorer.losing_races():
    print ""
    print "LOSER!"
    print "RaceID: %s, Type: %s" % (race.id(), race.type())
    print "RaceID\tRaceDate\tRaceType\tDistance\tDCat\tOdds\tRating\tStdDev\tPos\tSpeedL\tSpeedLF\tSpeedT\tSpeedTF\tSpeedSMA\tSpeedSMAD\tSpeedLMA\tSpeedLMAD\tAge\tWeightT\tWeightJA\tWeightOH"
    race.dump()
    #for runner in race.runners:
        #print "id=%-8s position=%-2s rating=%-15s std-dev=%-15s odds=%-5s heat=%s" % (runner.id(), runner.position(), runner.rating(), runner.std_dev(), runner.odds(), runner.rating()/runner.odds())
    print ""
print "%s winners out of %s or %s percent" % (scorer.winner_count(), scorer.total_count(), float(scorer.winner_count()) / scorer.total_count() * 100)
print "Profit of %s pounds from a stake of %s pounds per race" % (scorer.calculate_return(1.0), 1.0)
print ""
