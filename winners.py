from pymongo import MongoClient, DESCENDING
import datetime

db = MongoClient().racing_data

class Population:
    def __init__(self):
        self.population = []
    def add(self, items):
        if items != None:
            self.population.extend(items)
    def winner_count(self):
        count = 0.0
        for p in self.population:
            if p.winner_predicted():
                count += 1
        return count
    def total_count(self):
        return float(len(self.population))
    def calculate_return(self, stake, f = lambda r: True):
        total = 0.0
        pos_returns = []
        for r in self.population:
            if f(r):
                the_return = r.calculate_return(stake)
                if the_return > 0:
                    pos_returns.append(the_return)
                total += the_return
        print pos_returns
        return total

class Runner:
    def __init__(self):
        self.data = {}
    def id(self):
        return data["id"]
    def rating(self):
        return data["rating"]
    def std_dev(self):
        return data["std-dev"]
    def odds(self):
        data["odds"]
    def position(self):
        data["position"]
    def race_id(self):
        data["race-id"]
    def value(self):
        return self.rating() / self.odds()

class Runners:
    def __init__(self):
        self.runners = {}
    def winner_predicted(self):
        winner_rating = self.runners[1].rating()
        for x in xrange(2, len(self.runners) + 1):
            if winner_rating < self.runners[x].rating():
                return False
        return True
    def add(self, runner):
        self.runners[runner.position()] = runner
    def all_are_rated(self):
        for key in self.runners:
            if self.runners[key].rating() == 0:
                return False
        return True
    def calculate_return(self, stake):
        if self.winner_predicted():
            return self.runners[1].odds() * stake
        return -stake

def get_rating(runner, d):
    q = {"id": runner["horse"]["id"], "date":{"$lt":d}}
    rating_cursor = db.rating.find(q).sort("date", DESCENDING)
    if rating_cursor.count() >= 1:
        return rating_cursor[0]
    else:
        return {"rating": 0, "std-dev":0}

def get_population(d):
    races = db.cleaned_races.find({"date": d})
    race_list = []
    for race in races:
        runners = Runners()
        for runner in race["runners"]:
            rating = get_rating(runner, d)
            runners.add(Runner({
                "race-id": race["race-id"],
                "id": runner["horse"]["id"],
                "position": runner["position"],
                "rating": rating["rating"],
                "odds": runner["horse"]["odds"],
                "std-dev": rating["std-dev"]
                }))
        if runners.all_are_rated():
            race_list.append(runners)
    return race_list

d = datetime.datetime(2013, 8, 9)
population = Population()
for x in xrange(1,2):
    date = d-datetime.timedelta(x)
    population.add(get_population(date))

print "%s out of %s winners or %s percent" % (population.winner_count(), population.total_count(), population.winner_count() / population.total_count() * 100)
print "Return on 1 pounds level stake is %s pounds" % population.calculate_return(1.0)
print ""
for race in population.population:
    if race.winner_predicted():
        print "WINNER!"
    print "RaceID: %s" % race.runners[1].race_id()
    for runner in race.runners.itervalues():
        print "id=%-8s position=%-2s rating=%-15s std-dev=%-15s odds=%-5s value=%s" % (runner.id(), runner.position(), runner.rating(), runner.std_dev(), runner.odds(), runner.value())
    print ""
    print ""
