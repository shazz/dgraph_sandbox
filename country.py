import json
import sys

import pydgraph

# Drop All - discard all data and start from a clean slate.
def drop_all(client):
    return client.alter(pydgraph.Operation(drop_all=True))

def set_schema(client):
    schema = """
    name: string @index(exact) .
    has_state: [uid] .
    has_city: [uid] .
    zip: string .
    population: int .
    type Country {
        name
        has_state: [State]
    }
    type State {
        name
        has_city: [City]
    }
    type City {
        name
        zip
    }    

    """
    return client.alter(pydgraph.Operation(schema=schema))

# Create data using JSON.
def create_data(client):
    # Create a new transaction.
    txn = client.txn()
    try:
        # Create data.
        p = {
            "uid": "_:country:usa",
            "dgraph.type": "Country",
            "name": "United States of America",
            "has_state": [
                {
                    "uid": "_:state:massachussets",
                    "dgraph.type": "State",
                    "name": "Massachussets",
                    "has_city": [
                        {
                            "uid": "_:city:boston",
                            "dgraph.type": "City",
                            "name": "Boston",
                            "zip": "02108",
                            "population": 617594
                        },
                        {
                            "uid": "_:city:andover",
                            "dgraph.type": "City",
                            "name": "Andover",
                            "zip": "01810",
                            "population": 33201
                        }
                    ]
                },
                {
                    "uid": "_:state:vermont",
                    "dgraph.type": "State",
                    "name": "Vermont",
                    "has_city": [
                        {
                            "uid": "_:city:montpelier",
                            "dgraph.type": "City",
                            "name": "Montpelier",
                            "zip": "05601",
                            "population": 7855
                        },
                        {
                            "uid": "_:city:burlington",
                            "dgraph.type": "City",
                            "name": "Burlington",
                            "zip": "05401",
                            "population": 42211
                        }
                    ]
                }
            ]
        }

        # Run mutation.
        response = txn.mutate(set_obj=p)

        # Commit transaction.
        txn.commit()

        # Get uid of the outermost object
        # response.uids returns a map from blank node names to uids.
        print('Created Country with uid = {}'.format(response.uids['country:usa']))

    finally:
        # Clean up. Calling this after txn.commit() is a no-op and hence safe.
        txn.discard()

# Query for data.
def query_geography(client):
    # Run query.
    query = """{
        country(func: type(Country)) @filter ( has(has_state) )  {
            name
            nb_states: count(has_state)
            has_state @filter ( has(has_city) ) {
                name
                has_city @filter ( gt(population, 30000) ) {
                    name
                    population
                }
            }
        }
    }"""

    res = client.txn(read_only=True).query(query)
    equipments = json.loads(res.json)

    # Print results.
    print(json.dumps(equipments, sort_keys=False, indent=4))

if __name__ == '__main__':

    if len(sys.argv) != 2:
        raise ValueError('Please provide the dgraph server ip:port')
    
    try:
        print("Connect to server")
        client = pydgraph.DgraphClient(pydgraph.DgraphClientStub(sys.argv[1]))

        print("Deleting everything")
        drop_all(client)

        print("Creating schema")
        set_schema(client)

        print("Adding mutations")
        create_data(client)

        print("Getting country")
        query_geography(client)


    except Exception as e:
        print('Error: {}'.format(e))