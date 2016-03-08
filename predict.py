from collections import defaultdict
import networkx as nx
from database import Game, new_session


def team_strength(winner_losers):
    games_and_weights = defaultdict(int)
    for game in winner_losers:
        games_and_weights[game] += 1
    win_graph = nx.DiGraph()
    loss_graph = nx.DiGraph()
    for (winner, loser), weight in games_and_weights.iteritems():
        win_graph.add_edge(loser, winner, weight=weight)
        loss_graph.add_edge(winner, loser, weight=weight)
    loss_ranks = nx.pagerank(loss_graph)
    return {k: v - loss_ranks[k] for k, v in nx.pagerank(win_graph).iteritems()}
