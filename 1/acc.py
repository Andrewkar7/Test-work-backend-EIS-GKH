from pymongo import MongoClient
from json import loads
from pprint import pprint

# Cоздаем MongoClient для запуска экземпляра mongod, получаем доступ к базе
# данных и ее коллекциям
client = MongoClient('localhost', 27017)
db = client['Gkh-database']
accounts = db['accounts']

# Загружаем данные из json-файла
with open("account.metadata.json") as f:
    some_accounts = loads(f.read())

accounts.insert_many(some_accounts)

# Создаем запрос
aggr = [
    {"$unwind": "$sessions"},
    {"$unwind": "$sessions.actions"},
    {"$group": {"_id": {"number": "$number", "type": "$sessions.actions.type"},
                "created_at": {"$push": "$sessions.actions.created_at"},
                "count": {"$sum": 1}}},
    {"$group": {"_id": {"number": "$_id.number"},
                "actions": {"$push": {"type": "$_id.type", "last": {"$max": "$created_at"}, "count": "$count"}}}}
]

pprint(list(accounts.aggregate(aggr)))
