import pydgraph
import json
import data_parser

def create_schema(client):
    schema = data_parser.DGraphCSVLoader(client)
    schema.set_schema()

def create_data(client):
    loader = data_parser.DGraphCSVLoader(client)
    loader.load_data("data")
    
    
def drop_data(client):
    deletion = data_parser.DGraphCSVLoader(client)
    deletion.delete_all_data()

#queries

def find_influential_users(client, min_influence: float):
    query = """
    query influence($min_influence: float) {
        influentialUsers(func: gt(influenceScore, $min_influence)) {
            uid
            username
            influenceScore
            location
            popularPosts: posts @filter(gt(viewCount, 100)) {
                uid
                content
                viewCount
                engagementScore
                timestamp
            }
            postCount: count(posts)
        }
    }
    """
    variables = {'$min_influence': str(min_influence)}
    
    try:
        res = client.txn(read_only=True).query(query, variables=variables)
        result = json.loads(res.json)
        print(json.dumps(result, indent=4))
        return result
    except Exception as e:
        print(f"Error querying influential users: {str(e)}")
        return None

# TODO FIX THIS FUNCTION UPDATING THE REVERSE RELATIONSHIP OF HASHTAGS TO POSTS
def get_trending_hashtags(client, min_trend_score: float = 7.5, hashtag_limit: int = 5, post_limit: int = 3):
    """
        Find trending hashtags and related high-engagement posts
        Demonstrates: numeric indexing, multiple node types, ordering, pagination
        """
    query = """
        query trending($min_trend_score: float, $hashtag_limit: int, $post_limit: int) {
            trendingHashtags(func: gt(trendScore, $min_trend_score), first: $hashtag_limit) {
                uid
                name
                trendScore
                useCount
                relatedPosts: ~hashtags @filter(gt(engagementScore, 0.8)) 
                (first: $post_limit) {
                    uid
                    content
                    engagementScore
                    author {
                        username
                        influenceScore
                    }
                    likeCount: count(likedBy)
                    commentCount: count(comments)
                }
            }
        }
        """
    variables = {
            '$min_trend_score': str(min_trend_score),
            '$hashtag_limit': str(hashtag_limit),
            '$post_limit': str(post_limit)
    }
    
    try:
        res = client.txn(read_only=True).query(query, variables=variables)
        result = json.loads(res.json)
        print(json.dumps(result, indent=4))
        return result
    except Exception as e:
        print(f"Error querying trending hashtags: {str(e)}")
        return None
    

def query_community_members(client, community_name: str, page_size: int = 10, offset: int = 0):
        """
        Queries community members with pagination.
        
        Args:
            client: DGraph client instance
            community_name (str): Name of the community to query
            page_size (int): Number of members to return per page
            offset (int): Number of members to skip
            
        Returns:
            dict: JSON response containing community members
        """
        query = """
        query members($name: string, $first: string, $offset: string) {
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
            '$first': str(page_size),  # Convert to string
            '$offset': str(offset)     # Convert to string
        }

        try:
            res = client.txn(read_only=True).query(query, variables=variables)
            result = json.loads(res.json)
            print(json.dumps(result, indent=4))
            return result
        except Exception as e:
            print(f"Error querying community members: {str(e)}")
            return None

    
def get_user_network(client, min_influence: float = 8.0):
        """
        Find users followers and their engagement network
        Demonstrates: reverse relationships, multiple node types, count
        """
        query = """
        query network($min_influence: float) {
            activeUsers(func: gt(influenceScore, $min_influence)) {
                uid
                username
                influenceScore
                followers: ~follows {
                    uid
                    username
                    influenceScore
                    postCount: count(posts)
                    commentCount: count(comments)
                    followersCount: count(~follows)
                }
                followerCount: count(~follows)
                followingCount: count(follows)
            }
        }
        """
        variables = {
            '$min_influence':str( min_influence)
        }
        
        try:
            res = client.txn(read_only=True).query(query, variables=variables)
            result = json.loads(res.json)
            print(json.dumps(result, indent=4))
            return result
        except Exception as e:
            print(f"Error querying user network: {str(e)}")
            return None