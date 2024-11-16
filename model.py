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
