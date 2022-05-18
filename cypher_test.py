from cypher import CypherParser

match_query = """
MATCH (wallstreet:Movie {title: 'Wall Street'})<-[:ACTED_IN]-(actor)
RETURN actor.name
"""

def test_parse_gettok():
    parser = CypherParser()
    token = parser.gettok()

    assert token == None

def test_parse_match():
    parser = CypherParser()
    parser.parse(match_query)
    assert parser.__dict__ == {}
