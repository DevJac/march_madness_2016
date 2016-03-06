import os
import re
import difflib
import pandas as pd
from sqlalchemy import Column as SQAColumn
from sqlalchemy import create_engine, Integer, String, Date, Enum
from sqlalchemy.orm import sessionmaker
from sqlalchemy.ext.declarative import declarative_base


def Column(*args, **kwargs):
    kwargs.setdefault('nullable', False)
    return SQAColumn(*args, **kwargs)


Base = declarative_base()


class Game(Base):
    __tablename__ = 'game'

    team = Column(String, primary_key=True)
    opp = Column(String, primary_key=True)
    date = Column(Date, primary_key=True)
    result = Column(Enum('win', 'loss', name='win_loss'))
    points = Column(Integer)
    field_goals = Column(Integer)
    field_goal_attempts = Column(Integer)
    three_points = Column(Integer)
    three_point_attempts = Column(Integer)
    free_throws = Column(Integer)
    free_throw_attempts = Column(Integer)
    offensive_rebounds = Column(Integer)
    rebounds = Column(Integer)
    assists = Column(Integer)
    steals = Column(Integer)
    blocks = Column(Integer)
    turnovers = Column(Integer)
    fouls = Column(Integer)


engine = create_engine('postgresql+psycopg2://buttons:buttons@localhost/ncaa')
new_session = sessionmaker(engine)


def load_data():
    print 'load data'
    Base.metadata.create_all(engine)
    filename_teams = set()
    data_teams = set()
    for filename in os.listdir('data'):
        print filename
        filename_teams.add(re.match('([a-z-]+)201', filename).group(1))
        df = pd.read_csv(os.path.join('data', filename))
        try:
            data_teams.update(df['Opp'])
        except KeyError:
            pass
    print filename_teams
    print data_teams
    for team in data_teams:
        if type(team) == float:
            continue
        print team, difflib.get_close_matches(team, filename_teams)


if __name__ == '__main__':
    load_data()
