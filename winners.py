from pymongo import MongoClient, DESCENDING
import datetime

db = MongoClient().racing_data

class Population:
    def __init__(self, qualification = lambda race: True):
        self.qualification = qualification
        self.races_for_date = {}
    def results(self):
        return self.races_for_date.iteritems()
    def add(self, races):
        if races != None:
            for race in races:
                if race.date() in self.races_for_date:
                    self.races_for_date[race.date()].append(race)
                else:
                    self.races_for_date[race.date()] = Races(qualification)
    def qualifying_races(self):
        result = []
        for races in self.races_for_date.itervalues():
            result.extend(races.qualifying_races())
        return result
    def winner_count(self):
        count = 0.0
        for race in self.qualifying_races():
            if race.winner_predicted():
                count += 1
        return count
    def total_count(self):
        return len(self.qualifying_races())
    def calculate_return(self, stake):
        total = 0.0
        for race in self.qualifying_races():
            total += race.calculate_return(stake)
        return total

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
    def heat(self):
        return self.rating() / self.odds()

class Races:
    def __init__(self, qualification = lambda race: True):
        self.qualification = qualification
        self.races = []
    def append(self, race):
        self.races.append(race)
    def winner_count(self):
        count = 0
        for race in self.races:
            if race.winner_predicted():
                count += 1
        return count
    def qualifying_winner_count(self):
        count = 0
        for race in self.races:
            if race.winner_predicted() and race.is_qualifying(qualification):
                count += 1
        return count
    def qualifying_total_count(self):
        return len(self.qualifying_races())
    def total_count(self):
        return len(self.races)
    def qualifying_races(self):
        races = []
        for race in self.races:
            if race.is_qualifying(qualification):
                races.append(race)
        return races

class Race:
    def __init__(self, id, date, race_type):
        self.runners = {}
        self.race_id = id
        self.race_date = date
        self.race_type = race_type
    def id(self):
        return self.race_id
    def date(self):
        return self.race_date.strftime("%d-%m-%Y")
    def type(self):
        return self.race_type
    def winner(self):
        return self.runners[1]
    def highest_rated(self):
        highest = self.runners[1]
        for x in xrange(2, len(self.runners) + 1):
            if highest.rating() < self.runners[x].rating():
                highest = self.runners[x]
        return highest
    def winner_predicted(self):
        return self.winner().id() == self.highest_rated().id()
    def add(self, runner):
        self.runners[runner.position()] = runner
    def all_are_rated(self):
        for key in self.runners:
            if self.runners[key].rating() == 0:
                return False
        return True
    def calculate_return(self, stake):
        if self.winner_predicted():
            return self.winner().odds() * stake
        return -stake
    def is_qualifying(self, f):
        return f(self)

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
    for dbRace in races:
        race = Race(dbRace["race-id"], d, dbRace["race-type"])
        for runner in dbRace["runners"]:
            rating = get_rating(runner, d)
            race.add(Runner({
                "id": runner["horse"]["id"],
                "position": runner["position"],
                "rating": rating["rating"],
                "odds": runner["horse"]["odds"],
                "std-dev": rating["std-dev"]
                }))
        if race.all_are_rated():
            race_list.append(race)
    return race_list

d = datetime.datetime(2013, 8, 10)
#qualification = lambda race: True
#qualification = lambda race: race.highest_rated().odds() > 3.5
qualification = lambda race: race.highest_rated().heat() >= 4 and race.highest_rated().heat() <= 6.0
#qualification = lambda race: race.highest_rated().heat() >= 4 and race.highest_rated().heat() <= 6.0 and race.highest_rated().odds() > 3.5
population = Population(qualification)
for x in xrange(1,20):
#for x in xrange(1,2):
    date = d-datetime.timedelta(x)
    population.add(get_population(date))

print "%s out of %s winners or %s percent" % (population.winner_count(), population.total_count(), population.winner_count() / population.total_count() * 100)
print "Profit of %s pounds from a stake of %s pounds per race" % (population.calculate_return(1.0), 1.0)
print ""
current_date = datetime.datetime.now()
for date, results in population.results():
    print "Date: %s, qual %s / %s,  all %s / %s" % (date, results.qualifying_winner_count(), results.qualifying_total_count(), results.winner_count(), results.total_count())
    print ""
    for race in results.qualifying_races():
        if race.winner_predicted():
            print "WINNER!"
        print "RaceID: %s, Type: %s" % (race.id(), race.type())
        for runner in race.runners.itervalues():
            print "id=%-8s position=%-2s rating=%-15s std-dev=%-15s odds=%-5s heat=%s" % (runner.id(), runner.position(), runner.rating(), runner.std_dev(), runner.odds(), runner.heat())
        print ""
        print ""
