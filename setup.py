import json
import sys

import pydgraph

# Drop All - discard all data and start from a clean slate.
def drop_all(client):
    return client.alter(pydgraph.Operation(drop_all=True))

def set_schema(client):
    schema = """
    name: string @index(exact) .
    monitors_equipment: [uid] .
    load_type: string .
    model_type: string .
    equipment_type: [uid] .
    type Equipment {
        name
        model_type
        monitors_equipment: [Load]
        equipment_type: [EquipmentType]
    }    
    type Load {
        name
        load_type
    }
    type EquipmentType {
        name
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
            "uid": "_:equipment:mypowermeter",
            "dgraph.type": "Equipment",
            "name": "main power meter",
            "model_type": "Shazzy1234",
            "equipment_type": [
                    {
                        "uid": "_:equipmentType:PowerMeter",
                        "name":"Power Meter"
                    }
                ],            
            'monitors_equipment': [
                {
                    "uid": "_:load:fridge",
                    "dgraph.type": "Load",
                    "name": "Fridge 1",
                    "load_type": "appliance"
                },
                {
                    "uid": "_:load:microwave",
                    "dgraph.type": "Load",
                    "name": "Microwave 1",
                    "load_type": "appliance"
                },
                {
                    "uid": "_:load:light",
                    "dgraph.type": "Load",
                    "name": "Ceiling lights",
                    "load_type": "light"
                }
            ]
        }

        # Run mutation.
        response = txn.mutate(set_obj=p)

        # Commit transaction.
        txn.commit()

        # Get uid of the outermost object
        # response.uids returns a map from blank node names to uids.
        print('Created Equipment with uid = {}'.format(response.uids['equipment:mypowermeter']))

    finally:
        # Clean up. Calling this after txn.commit() is a no-op and hence safe.
        txn.discard()

# Query for data.
def query_equipments(client):
    # Run query.
    query_ok = """{
        equipments(func: type(Equipment)) {
            name
            model_type
            count(equipment_type)
            equipment_type @filter (has(equipment_type)) {
                name
            }
            count(monitors_equipment)
            monitors_equipment @filter (eq(count(monitors_equipment), 0) AND (eq(load_type, "light")) ) {
              uid
              name
              load_type
            }        
        }
    }"""

    query_nok = """{
        equipments(func: type(Equipment)) {
            name
            model_type
            count(equipment_type)
            equipment_type @filter (has(equipment_type)) {
                name
            }            
            count(monitors_equipment)
            monitors_equipment @filter ( has(monitors_equipment) AND (eq(load_type, "light")) ) {
              uid
              name
              load_type
            }        
        }
    }"""

    res = client.txn(read_only=True).query(query_ok)
    equipments = json.loads(res.json)

    # Print results.
    print(json.dumps(equipments, sort_keys=True, indent=4))

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

        print("Getting equipments")
        query_equipments(client)


    except Exception as e:
        print('Error: {}'.format(e))
