import re

class CypherParser():
    def __init__(self):
        self.last_char = " "
        self.pos = 0
        self.end = False
        self.graph = []
        self.return_clause = []
        self.match = False
        self.merge = False
   

    def getchar(self):
        
        char = self.statement[self.pos]
        if self.pos + 1 == len(self.statement):
            self.end = True
            return char
        self.pos = self.pos + 1
        
        return char
        
    def gettok(self):
        while (self.end == False and (self.last_char == " " or self.last_char == "\n")):
            self.last_char = self.getchar()
        
        
              
        if self.last_char == "(":
            self.last_char = self.getchar()
            return "openbracket"
        
        if self.last_char == ")":
            self.last_char = self.getchar()
            return "closebracket"
        
        if self.last_char == "*":
            self.last_char = self.getchar()
            return "wildcard"
        
        if self.last_char == "'":
            self.last_char = self.getchar()
            identifier = ""
            while self.end == False and self.last_char != "'":
                if self.last_char == "\\":
                    self.last_char = self.getchar()
                identifier = identifier + self.last_char
                self.last_char = self.getchar()
            if self.end and self.last_char != ")" and self.last_char != "'":
                identifier += self.last_char
            
            self.last_char = self.getchar()
            
            return identifier
        
        if re.match("[a-zA-Z0-9\.\_]+", self.last_char):
            identifier = ""
            while self.end == False and re.match("[a-zA-Z0-9\.\_]+", self.last_char):
                
                identifier = identifier + self.last_char
                self.last_char = self.getchar()
            
            if self.end and self.last_char != ")":
                identifier += self.last_char
            
            return identifier
    
        if self.last_char == "=":
            self.last_char = self.getchar()
            return "eq"
        
        if self.last_char == "~":
            self.last_char = self.getchar()
            return "tilde"
        
        if self.last_char == ",":
            self.last_char = self.getchar()
            return "comma"
        
        if self.last_char == ":":
            self.last_char = self.getchar()
            return "separator"
        
        if self.last_char == "{":
            self.last_char = self.getchar()
            return "opencurly"
        
        if self.last_char == "}":
            self.last_char = self.getchar()
            return "closecurly"
        
        if self.last_char == "<":
            self.last_char = self.getchar()
            return "leftarrow"
        
        if self.last_char == ">":
            self.last_char = self.getchar()
            return "rightarrow"
        
        if self.last_char == "-":
            self.last_char = self.getchar()
            return "dash"
        
        if self.last_char == "[":
            self.last_char = self.getchar()
            return "squareopen"
        
        if self.last_char == "]":
            self.last_char = self.getchar()
            return "squareclose"
        
        if self.end:
            return None
# MATCH (wallstreet:Movie {title: 'Wall Street'})<-[:ACTED_IN]-(actor)
# RETURN actor.name
    def parse(self, statement):
        self.statement = statement
        token = self.gettok()
        if token == "match":
            self.parse_match()
        elif token == "merge":
            self.parse_merge()
    
    def parse_attribute(self):
        attribute_name = self.gettok()
        separator = self.gettok()
        attribute_value = self.gettok()
        
        self.graph[-1]["attributes"][attribute_name] = attribute_value
        print("New attribute: {} = {}".format(attribute_name, attribute_value))
        
        token = self.gettok()
        if token == "comma":
            self.parse_attribute()
    
    def parse_relationship(self, token, callback):
        arrow_at_end = False
        if token == "dash":
            arrow_at_end = True
        if token == "leftarrow":
            dash = self.gettok()
            print("dash " + dash)
        
        openbracket = self.gettok()
        print(openbracket)
        separator = self.gettok()
        relationship_name = self.gettok()
        print(relationship_name)
        closebracket = self.gettok()
        dash = self.gettok()
        if arrow_at_end:
            rightarrow = self.gettok()
            print(rightarrow)
        self.graph.append({
            "kind": "relationship",
            "name": relationship_name
        })
        
        callback()
    
    def parse_return(self):
        variable = self.gettok()
        self.return_clause.append(variable)
        token = self.gettok()
        if token == "comma":
            self.parse_return()
    
    def parse_remainder(self, token):
        
        if token == "return":
            self.parse_return()
    
    def parse_relationship_begin(self, token, callback):
        print("leftordash " + token)
        if token == "leftarrow" or token == "dash":
            self.parse_relationship(token, callback)
        if token == "comma":
            callback()

    def parse_match(self):
        self.match = True
        token = self.gettok()
        print(token)
        if token == "openbracket":
            variable = self.gettok()
            print(variable)
            token = self.gettok()
            print("nexttoken " + token)
            self.graph.append({
                "kind": "match",
                "variable": variable
            })
            
            if token == "separator":
                # we have a label
                print(token)
                label = self.gettok()

                self.graph[-1]["label"] = label
                self.graph[-1]["attributes"] = {}

                print(label)
                token = self.gettok()
                print(token)
                if token == "opencurly": # begin attributes

                    self.parse_attribute()
                    print("begin attributes " + token)
                    closebracket = self.gettok()
            
                token = self.gettok()
                print("parsed a label " + token)
                if token == "return":
                    self.parse_remainder(token)
                else:
                    self.parse_relationship_begin(token, self.parse_match)
            
            if token == "closebracket":
                token = self.gettok()
                print("innertoken " + token)
                if token == "comma":
                    self.parse_match()
                elif token == "dash" or token == "leftarrow":
                    self.parse_relationship_begin(token, self.parse_match)
                else:
                    self.parse_remainder(token)
    
    def parse_merge(self):
        self.merge = True
        token = self.gettok()
        print(token)
        if token == "openbracket":
            variable = self.gettok()
            print(variable)
            token = self.gettok()
            print("nexttoken " + token)
            self.graph.append({
                "kind": "merge",
                "variable": variable
            })
            
            if token == "separator":
                # we have a label
                print(token)
                label = self.gettok()

                self.graph[-1]["label"] = label
                self.graph[-1]["attributes"] = {}

                print(label)
                token = self.gettok()
                print(token)
                if token == "opencurly": # begin attributes

                    self.parse_attribute()
                    print("begin attributes " + token)
                    closebracket = self.gettok()
            
                token = self.gettok()
                print("parsed a label " + token)
                if token == "return":
                    self.parse_remainder(token)
                else:
                    self.parse_relationship_begin(token, self.parse_merge)
            
            if token == "closebracket":
                token = self.gettok()
                print("innertoken " + token)
                if token == "comma":
                    self.parse_match()
                elif token == "dash" or token == "leftarrow":
                    self.parse_relationship_begin(token, self.parse_merge)
                else:
                    self.parse_remainder(token)
            
            

