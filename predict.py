from collections import defaultdict
from datetime import date
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
