from cypher import CypherParser

match_query = """
MATCH (actor)-[:ACTED_IN]->(wallstreet:Movie {title: 'Wall Street'}) RETURN actor
"""

def test_parse_match():
    parser = CypherParser()
    parser.parse(match_query)
    assert parser.__dict__ == {}
