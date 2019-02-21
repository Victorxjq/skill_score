import pydgraph
import config
import os
import json

def check_node_entity_id(client, entity_id):
    query = """{
          everyone(func: eq(entity_id, "%s")) {
            uid
            entity_id
            name
            alias
            mentions
          }
        }""" % entity_id
    res = client.txn().query(query)
    data = res.json.decode("utf-8")
    data = json.loads(data)
    data = data["everyone"]
    if len(data) > 0:
        return data[0]
    else:
        return None



if __name__ == '__main__':
    # upload into graph/develop
    config.URL = '192.168.1.210:9081'
    # config.URL = '192.168.1.36:9081'
    client_stub = pydgraph.DgraphClientStub(config.URL)
    client = pydgraph.DgraphClient(client_stub)
    path_input = os.path.join(os.path.dirname(__file__), r'skill_entity_update_20190220.jsonl')
    p_del = []
    p_update = []
    with open(path_input) as input_file:
        for line in input_file.readlines():
            line=line.strip('\n')
            line=json.loads(line)
            query=check_node_entity_id(client,line['id'])
            if 'alias' in query.keys():
                if 'mentions' in query.keys():
                    p1 = {
                        'uid': query['uid'],
                        'mentions': query['mentions'],
                        'alias': query['alias']
                    }
                    p_del.append(p1)
                else:
                    p1 = {
                        'uid': query['uid'],
                        'alias': query['alias']
                    }
                    p_del.append(p1)
            if 'mentions' in line.keys():
                p2={
                    'uid': query['uid'],
                    'name':line['name'],
                    'mentions': line['mentions'],
                    'alias': line['alias']
                }
                p_update.append(p2)
            else:
                p2={
                    'uid': query['uid'],
                    'name': line['name'],
                    'alias': line['alias']
                }
                p_update.append(p2)

    #新增一个实体
    p_manual={
        'uid': "_:%s" % "BUG跟踪",
        'name': "BUG跟踪",
        'entity_id': '18795',
        'alias': ["bug跟踪", "BUG追踪", "bug追踪"]
    }
    p_update.append(p_manual)
    # print(p_del)
    # print(p_update)
    # # 清空alias及metions
    txn = client.txn()
    try:
        txn.mutate(del_obj=p_del)
        txn.commit()
    finally:
        txn.discard()

    # 更新新的alias及mentions
    txn = client.txn()
    try:
        txn.mutate(set_obj=p_update)
        txn.commit()
    finally:
        txn.discard()
