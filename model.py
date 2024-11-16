import pydgraph
import os
import json
import datetime
import data_parser


def create_data(client):
    loader = data_parser.DGraphCSVLoader(client)
    loader.set_schema()
    loader.load_data("./data")
    
    
def drop_data(client):
    deletion = data_parser.DGraphCSVLoader(client)
    deletion.delete_data()

#queries
def query_user(client, username: str):
    query = """
        query getUser($usr: string) {
            user(func: eq(username, $usr)) {
                username
                email
                follows {
                    username 
                }
                posts {
                    content
                    timestamp
                }
                communities {
                    name
                    category
                }
                comments {
                    content
                    timestamp
                }
        }
        """
    variables = {'$usr': username}
    
    res = client.txn(read_only=True).query(query, variables=variables)
    ppl = json.loads(res.json)
    
    # print results
    print(json.dumps(ppl, indent=4))
    

def query_follower_network(client, username: str):
    """
    Queries both followers and following for a user (demonstrates reverse edge).
    """
    query = """
        query network($username: string) {
            user(func: eq(username, $username)) {
                username
                follows {
                    username
                }
                ~follows {  # Reverse edge query
                    username
                }
            }
        }
    """
    variables = {'$username': username}
    
    res = client.txn(read_only=True).query(query, variables=variables)
    ppl = json.loads(res.json)
    
    # print results
    print(json.dumps(ppl, indent=4))
    

def query_community_members(client, community_name: str, page_size: int = 10, offset: int = 0):
        """
        Queries community members with pagination.
        """
        query = """
        query members($name: string, $first: int, $offset: int) {
            community(func: eq(name, $name)) {
                name
                memberCount
                members(first: $first, offset: $offset) {
                    username
                    influenceScore
                }
            }
        }
        """
        variables = {
            '$name': community_name,
            '$first': page_size,
            '$offset': offset
        }
    
        res = client.txn(read_only=True).query(query, variables=variables)
        ppl = json.loads(res.json)
        
        # print results
        print(json.dumps(ppl, indent=4)) 


def query_by_text(client, search_text: str):
    """
    Queries posts and comments using text search.
    """
    query = """
        query search($text: string) {
            posts(func: anyoftext(content, $text)) {
                content
                timestamp
                author {
                    username
                }
            }
            
            comments(func: anyoftext(content, $text)) {
                content
                timestamp
                author {
                    username
                }
            }
        }
    """
    variables = {'$text': search_text}
    
    res = client.txn(read_only=True).query(query, variables=variables)
    ppl = json.loads(res.json)
    print(json.dumps(ppl, indent=4))
    
    
def query_posts_by_user(client, username: str):
    """
    Queries posts by a user.
    """
    query = """
        query posts($username: string) {
            user(func: eq(username, $username)) {
                posts {
                    content
                    timestamp
                }
            }
        }
    """
    variables = {'$username': username}
    
    res = client.txn(read_only=True).query(query, variables=variables)
    ppl = json.loads(res.json)
    print(json.dumps(ppl, indent=4))