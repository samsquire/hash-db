from cypher import CypherParser

def test_parse_match():
    match_query = "match (actor)-[:ACTED_IN]->(wallstreet:Movie {title: 'Wall Street'}) return actor"
    parser = CypherParser()
    parser.parse(match_query)

    expected_graph = [
        {'kind': 'match',
         'variable': 'actor'
        },
        {'kind': 'relationship',
         'name': 'ACTED_IN'
        },
        {'attributes': {'title': 'Wall Street'},
         'kind': 'match',
         'label': 'Movie',
         'variable': 'wallstreet'
        }
    ]

    assert parser.statement == match_query
    assert parser.graph == expected_graph

def test_parse_match_case_insensitive():
    match_query = "MATCH (actor)-[:ACTED_IN]->(wallstreet:Movie {title: 'Wall Street'}) RETURN actor"
    parser = CypherParser()
    parser.parse(match_query)

    expected_graph = [
        {'kind': 'match',
         'variable': 'actor'
        },
        {'kind': 'relationship',
         'name': 'ACTED_IN'
        },
        {'attributes': {'title': 'Wall Street'},
         'kind': 'match',
         'label': 'Movie',
         'variable': 'wallstreet'
        }
    ]

    assert parser.statement == match_query
    assert parser.graph == expected_graph
