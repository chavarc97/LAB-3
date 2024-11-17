import os
import pydgraph
import model

"""
*********************************************************************************************************************
*************************************** CONCLUSION ******************************************************************
*********************************************************************************************************************
FOR THIS PROJECT, I HAVE CREATED A CLIENT.PY FILE THAT WILL BE USED TO INTERACT WITH THE DGRAPH DATABASE.
IT WAS A BIT CHALLENGING TO ORGANIZE THE CODE IN A WAY THAT IT WILL BE EASY TO READ AND UNDERSTAND.
ALSO, I HAD TO MAKE SURE THAT THE CODE IS WELL COMMENTED AND THE FUNCTIONS ARE WELL DOCUMENTED.
I ENCOUNTER A FEW ERRORS WHILE TESTING THE CODE, BUT I WAS ABLE TO FIX THEM.
BUT AT THE END EVERYTHING WORKED FINE AND I WAS ABLE TO CREATE A CLIENT.PY FILE THAT WILL BE USED TO INTERACT WITH THE DGRAPH DATABASE.

In conclusion, this project has allowed me to gain practical experience with DGraph, a powerful graph database, 
and its capabilities in managing and querying complex data structures. Throughout the development process, 
I focused on creating and maintaining a schema, loading and deleting data, and implementing various queries 
to retrieve influential users, trending hashtags, and user networks. The project has helped me improve my understanding 
of graph-based data modeling, including reverse relationships and numeric indexing. It also deepened my skills in handling
large datasets and efficiently querying them using DGraph's query language. Overall, this project was a valuable learning experience, 
and I look forward to applying these skills in future projects.
"""


DGRAPH_URI = os.getenv('DGRAPH_URI', 'localhost:9080')



def print_menu():
    mm_options = {
        1: "Create data",
        2: "Query data",
        3: "Delete users with influenceScore less than a given threshold. i.e. > 5.0",
        4: "Drop All",
        5: "Exit",
    }
    for key in mm_options.keys():
        print(key, '--', mm_options[key])

def create_client_stub():
    return pydgraph.DgraphClientStub(DGRAPH_URI)


def create_client(client_stub):
    return pydgraph.DgraphClient(client_stub)


def close_client_stub(client_stub):
    client_stub.close()


def query_menu(client):

    mm_options = {
        1: "Find Influential Users",
        2: "Get trending hashtags",
        3: "Query Community Members",
        4: "Find users followers and their engagement network",
    }
    for key in mm_options.keys():
        print(key, '--', mm_options[key])
    opc = int(input('Enter your choice: '))
    match opc:
        case 1:
            print('*************************************************************************')
            print('\t\t\tFind Influential Users')
            print('*************************************************************************')
            influenceScore = 8.0
            model.find_influential_users(client, influenceScore)
        case 2:
            print('*************************************************************************')
            print('\t\t\tGet trending hashtags')
            print('*************************************************************************')
            model.get_trending_hashtags(client)
        case 3:
            print('*************************************************************************')
            print('\t\t\tQuery Community Members')
            print('*************************************************************************')
            communities = {
                1: "Tech Innovators",
                2: "JS Developers",
                3: "Data Science Hub",
                4: "Cloud Computing",
                5: "AI Research"
            }
            print('Available communities:')
            for community in communities:
                print(community, '--', communities[community])
            opc = int(input('Enter your choice: '))
            model.query_community_members(client, communities[opc])
        case 4:
            print('*************************************************************************')
            print('\t\t\tFind users followers and their engagement network')
            print('*************************************************************************')
            model.get_user_network(client)
        case _:
            print('Invalid option')
    

def main():
    # Init Client Stub and Dgraph Client
    client_stub = create_client_stub()
    client = create_client(client_stub)

    # Create schema
    model.create_schema(client)
    
    # menu loop
    while(True):
        print_menu()
        option = int(input('Enter your choice: '))
        if option == 1:
            model.create_data(client)
        if option == 2:
            query_menu(client)
        if option == 3:
            model.delete_condition(client)
        if option == 4:
            model.drop_data(client)
        if option == 5:
            model.drop_data(client)
            close_client_stub(client_stub)
            exit(0)


if __name__ == '__main__':
    try:
        main()
    except Exception as e:
        print('Error: {}'.format(e))