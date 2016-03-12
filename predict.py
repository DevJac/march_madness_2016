import datetime
import itertools
from collections import defaultdict
from datetime import date
from sqlalchemy import func
import networkx as nx
from database import Game, new_session


def team_strength(winner_losers):
    games_and_weights = defaultdict(int)
    for winner, loser, weight in winner_losers:
        games_and_weights[winner, loser] += weight
    win_graph = nx.DiGraph()
    loss_graph = nx.DiGraph()
    for (winner, loser), weight in games_and_weights.iteritems():
        win_graph.add_edge(loser, winner, weight=weight)
        loss_graph.add_edge(winner, loser, weight=weight)
    loss_ranks = nx.pagerank(loss_graph)
    return {k: v - loss_ranks[k] for k, v in nx.pagerank(win_graph).iteritems()}


def print_strongest_teams():
    session = new_session()
    r = session.query(Game).filter(Game.result == 'win',
                                   Game.date > date(2015, 6, 1))
    wl = [(g.team, g.opponent, 3 + min(5, g.points - g.opp.points))
          for g in r]
    ts = team_strength(wl)
    for team, strength in sorted(ts.iteritems(),
                                 key=lambda a: a[1],
                                 reverse=True)[:100]:
        print '{} {:.0f}'.format(team, strength*10000)


def known_game_features(game):
    features = [
        game.points,
        game.field_goals,
        game.field_goal_attempts,
        game.three_points,
        game.three_point_attempts,
        game.free_throws,
        game.free_throw_attempts,
        game.offensive_rebounds,
        game.rebounds,
        game.assists,
        game.steals,
        game.blocks,
        game.turnovers,
        game.fouls,
        game.opp.points,
        game.opp.field_goals,
        game.opp.field_goal_attempts,
        game.opp.three_points,
        game.opp.three_point_attempts,
        game.opp.free_throws,
        game.opp.free_throw_attempts,
        game.opp.offensive_rebounds,
        game.opp.rebounds,
        game.opp.assists,
        game.opp.steals,
        game.opp.blocks,
        game.opp.turnovers,
        game.opp.fouls]
    return features, game.result


def game_features(game):
    features = (
        func.coalesce(func.avg(Game.points), 0),
        func.coalesce(func.avg(Game.field_goals), 0),
        func.coalesce(func.avg(Game.field_goal_attempts), 0),
        func.coalesce(func.avg(Game.three_points), 0),
        func.coalesce(func.avg(Game.three_point_attempts), 0),
        func.coalesce(func.avg(Game.free_throws), 0),
        func.coalesce(func.avg(Game.free_throw_attempts), 0),
        func.coalesce(func.avg(Game.offensive_rebounds), 0),
        func.coalesce(func.avg(Game.rebounds), 0),
        func.coalesce(func.avg(Game.assists), 0),
        func.coalesce(func.avg(Game.steals), 0),
        func.coalesce(func.avg(Game.blocks), 0),
        func.coalesce(func.avg(Game.turnovers), 0),
        func.coalesce(func.avg(Game.fouls), 0))
    session = new_session()
    team_stats = (
        session
        .query(*features)
        .filter(Game.team == game.team,
                Game.date < game.date,
                Game.date > game.date - datetime.timedelta(days=30*6))
        .one())
    opponent_stats = (
        session
        .query(*features)
        .filter(Game.team == game.opponent,
                Game.date < game.date,
                Game.date > game.date - datetime.timedelta(days=30*6))
        .one())
    all_past_games = (
        session
        .query(Game)
        .filter(Game.result == 'win',
                Game.date < game.date,
                Game.date > game.date - datetime.timedelta(days=30*6)))
    ts = team_strength(
        (g.team, g.opponent, 3 + min(5, g.points - g.opp.points))
         for g in all_past_games)
    return tuple(itertools.chain.from_iterable([
        (float(i) for i in team_stats),
        (float(i) for i in opponent_stats),
        [ts[game.team] * 10000],
        [ts[game.opponent] * 10000]]))
