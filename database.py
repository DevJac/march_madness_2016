import os
import re
import difflib
from datetime import datetime, timedelta
from tqdm import tqdm
import pandas as pd
from sqlalchemy import Column as SQAColumn, and_
from sqlalchemy import create_engine, Integer, String, Date, Enum
from sqlalchemy.orm import sessionmaker, relationship, remote, foreign
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.exc import DataError, IntegrityError


def Column(*args, **kwargs):
    kwargs.setdefault('nullable', False)
    return SQAColumn(*args, **kwargs)


Base = declarative_base()


class Game(Base):
    __tablename__ = 'game'

    team = Column(String, primary_key=True, index=True)
    opponent = Column(String, primary_key=True, index=True)
    date = Column(Date, primary_key=True, index=True)
    result = Column(Enum('win', 'loss', name='win_loss'), index=True)
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
    opp = relationship(
        'Game',
        uselist=False,
        primaryjoin=and_(
            foreign(team) == remote(opponent),
            foreign(opponent) == remote(team),
            foreign(date) == remote(date)))


engine = create_engine('postgresql://buttons:buttons@localhost/ncaa')
new_session = sessionmaker(engine)


def load_data():
    win_loss_map = {'W': 'win', 'L': 'loss'}
    school_name_map = {}
    with open('school_name_map.txt') as f:
        for line in f:
            m = re.match('([a-z-]+) (.*)', line)
            school_name_map[m.group(1)] = m.group(2)
    Base.metadata.drop_all(engine)
    Base.metadata.create_all(engine)
    session = new_session()
    for filename in tqdm(os.listdir('data')):
        team = school_name_map[re.match('[a-z-]+', filename).group(0)]
        df = pd.read_csv(os.path.join('data', filename))
        for row in df.itertuples():
            session.add(Game(team=team,
                             opponent=row.Opp,
                             date=datetime.strptime(
                                row.Date, '%Y-%m-%d').date(),
                             result=win_loss_map[row._6[0]],
                             points=row.Tm,
                             field_goals=row.FG,
                             field_goal_attempts=row.FGA,
                             three_points=row._12,
                             three_point_attempts=row._13,
                             free_throws=row.FT,
                             free_throw_attempts=row.FTA,
                             offensive_rebounds=row.ORB,
                             rebounds=row.TRB,
                             assists=row.AST,
                             steals=row.STL,
                             blocks=row.BLK,
                             turnovers=row.TOV,
                             fouls=row.PF))
            try:
                session.commit()
            except (DataError, IntegrityError):
                session.rollback()
    bad_count = 0
    total_games = session.query(Game).count()
    for game in tqdm(session.query(Game), total=total_games):
        if not session.query(Game).filter_by(team=game.opponent,
                                             opponent=game.team,
                                             date=game.date).one_or_none():
            bad_count += 1
            session.delete(game)
    session.commit()
    print '{:,d} / {:,d} bad games'.format(bad_count, total_games)


if __name__ == '__main__':
    load_data()
