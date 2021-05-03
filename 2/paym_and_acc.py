from pymongo import MongoClient
from json import loads
from pprint import pprint

# Cоздаем MongoClient для запуска экземпляра mongod, получаем доступ к базе
# данных и ее коллекциям
client = MongoClient('localhost', 27017)
db = client['pay-database']
accruals = db['accruals']
payments = db['payments']

# Загружаем данные из json-файлов
with open("accrual.metadata.json") as f:
    some_accruals = loads(f.read())

accruals.insert_many(some_accruals)

with open("payment.metadata.json") as g:
    some_payments = loads(g.read())

payments.insert_many(some_payments)


def mapping(accrual, payment):
    """ функция, которая сделает запрос к платежам и найдёт для каждого платежа долг,
    который будет оплачен, а также список платежей, которые не нашли себе долг."""
    # Сортируем данные, чтобы удобнее было перебирать данные
    accrual = list(accruals.aggregate([{'$sort': {'date': -1}}]))
    payment = list(payments.aggregate([{'$sort': {'date': 1}}]))

    # Создаем пустые списки, в которые будем отправлять соответствия и наоборот
    list_of_matches = []
    list_of_no_matches = []

    for paym in payment:
        for accr in accrual:
            if paym['date'] > accr['date']:
                if paym['month'] == accr['month']:
                    paym['accrual'] = accr
                    list_of_matches.append(paym)
                    accrual.remove(accr)
                else:
                    paym['accrual'] = accrual[-1]
                    list_of_matches.append(paym)
                    accrual.pop()
                break

    # Делаем проверку, есть ли платежи, не нашедшие себе долг
    if len(payment) > len(list_of_matches):
        while len(payment) > len(list_of_matches):
            list_of_no_matches.append(payment[-1])
            payment.remove(payment[-1])

    print('Таблица найденных соответствий: ')
    pprint(list_of_matches)
    print('Список платежей, которые не нашли себе долг: ')
    pprint(list_of_no_matches)


mapping(accruals, payments)
