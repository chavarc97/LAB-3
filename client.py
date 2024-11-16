import os
import pydgraph
import model


DGRAPH_URI = os.getenv('DGRAPH_URI', 'localhost:9080')



def print_menu():
    mm_options = {
        1: "Create data",
        2: "Query data",
        3: "Delete person",
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
        1: "Query User",
        2: "Query Follower Network",
        3: "Query Community Members",
        4: "Query Content by Text",
        5: "Query Posts by User",
    }
    for key in mm_options.keys():
        print(key, '--', mm_options[key])
    opc = int(input('Enter your choice: '))
    match opc:
        case 1:
            print('*************************************************************************')
            print('\t\t\tQuery User:')
            print('*************************************************************************')
            model.query_user(client, input('Enter username: '))
        case 2:
            print('*************************************************************************')
            print('\t\t\tQuery Follower Network')
            print('*************************************************************************')
            model.query_follower_network(client, input('Enter username: '))
        case 3:
            print('*************************************************************************')
            print('\t\t\tQuery Community Members')
            print('*************************************************************************')
            model.query_community_members(client, input('Enter community name: '))
        case 4:
            print('*************************************************************************')
            print('\t\t\tQuery Content by Text')
            print('*************************************************************************')
            model.query_content_by_text(client, input('Enter text: '))
        case 5:
            print('*************************************************************************')
            print('\t\t\tQuery Posts by User')
            print('*************************************************************************')
            model.query_posts_by_user(client, input('Enter username: '))
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
            pass
        if option == 4:
            model.drop_data(client)
        if option == 5:
            model.drop_all(client)
            close_client_stub(client_stub)
            exit(0)


if __name__ == '__main__':
    try:
        main()
    except Exception as e:
        print('Error: {}'.format(e))