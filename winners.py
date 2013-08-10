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

class Runners:
    def __init__(self):
        self.runners = {}
    def winner_predicted(self):
        winner_rating = self.runners[1]["rating"]
        for x in xrange(2, len(self.runners) + 1):
            if winner_rating < self.runners[x]["rating"]:
                return False
        return True
    def add(self, runner):
        self.runners[runner["position"]] = runner
    def all_are_rated(self):
        for key in self.runners:
            if self.runners[key]["rating"] == 0:
                return False
        return True

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
            runners.add({"id": runner["horse"]["id"], "position": runner["position"], "rating": rating["rating"], "std-dev": rating["std-dev"]})
        if runners.all_are_rated():
            race_list.append(runners)
    return race_list

d = datetime.datetime(2013, 8, 9)
population = Population()
for x in xrange(1,99):
    date = d-datetime.timedelta(x)
    population.add(get_population(date))

print "%s out of %s winners or %s percent" % (population.winner_count(), population.total_count(), population.winner_count() / population.total_count() * 100)

#for race in population.population:
    #if race.winner_predicted():
        #print "WINNER!"
    #for runner in race.runners.itervalues():
        #print "id=%-8s position=%-4s rating=%-15s std-dev=%s" % (runner["id"], runner["position"], runner["rating"], runner["std-dev"])
    #print ""
    #print ""
