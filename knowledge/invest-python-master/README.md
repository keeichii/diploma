# T-Invest

[![PyPI](https://img.shields.io/gitlab/v/release/238?gitlab_url=https%3A%2F%2Fopensource.tbank.ru)](https://opensource.tbank.ru/invest/invest-python/-/packages)
[![PyPI - Python Version](https://img.shields.io/python/required-version-toml?tomlFilePath=https://opensource.tbank.ru/invest/invest-python/-/raw/master/pyproject.toml)](https://www.python.org/downloads/)
[![Opensource](https://img.shields.io/gitlab/license/238?gitlab_url=https%3A%2F%2Fopensource.tbank.ru)](https://opensource.tbank.ru/invest/invest-python/-/blob/master/LICENSE)

[//]: # (![PyPI - Downloads]&#40;https://img.shields.io/pypi/dw/t-investments&#41;)

Данный репозиторий предоставляет клиент для взаимодействия с торговой платформой 
[Т-Инвестиции](https://www.tbank.ru/invest/) на языке Python.

Проект является продуктом независимой разработки и не связан с какими-либо компаниями. Библиотека распространяется 
свободно и не является коммерческим продуктом или официальным выпуском какого-либо стороннего разработчика. Все 
исходные материалы, архитектура и реализация созданы самостоятельно.

The project is the result of independent development and is not affiliated with any companies. The library is 
distributed freely and is not a commercial product or official release of any third-party vendor. All source materials,
architecture, and implementation were created independently.


- [Документация](https://opensource.tbank.ru/invest/invest-python/-/blob/master/README.md?ref_type=heads)
- [Документация по Invest API](https://developer.tbank.ru/invest/intro/intro)

## Начало работы

<!-- terminal -->

```
$ pip install t-tech-investments --index-url https://opensource.tbank.ru/api/v4/projects/238/packages/pypi/simple
```

## Возможности

- &#9745; Синхронный и асинхронный GRPC клиент
- &#9745; Возможность отменить все заявки
- &#9745; Выгрузка истории котировок "от" и "до"
- &#9745; Кеширование данных
- &#9745; Торговая стратегия

## Как пользоваться

### Получить список аккаунтов

```python
from t_tech.invest import Client

TOKEN = 'token'

with Client(TOKEN) as client:
    print(client.users.get_accounts())
```

### Переопределить target

В T-Invest API есть два контура - "боевой", предназначенный для исполнения ордеров на бирже и "песочница", 
предназначенный для тестирования API и торговых гипотез, заявки с которого не выводятся на биржу, 
а исполняются в эмуляторе.

Переключение между контурами реализовано через target, INVEST_GRPC_API - "боевой", INVEST_GRPC_API_SANDBOX - "песочница"

```python
from t_tech.invest import Client
from t_tech.invest.constants import INVEST_GRPC_API

TOKEN = 'token'

with Client(TOKEN, target=INVEST_GRPC_API) as client:
    print(client.users.get_accounts())
```

> :warning: **Не публикуйте токены в общедоступные репозитории**
<br/>

Остальные примеры доступны в [examples](https://opensource.tbank.ru/invest/invest-python/-/tree/master/examples).

## Contribution

Для тех, кто хочет внести свои изменения в проект.

- [CONTRIBUTING](https://opensource.tbank.ru/invest/invest-python/-/blob/master/CONTRIBUTING.md)

## License

Лицензия [The Apache License](https://opensource.tbank.ru/invest/invest-python/-/blob/master/LICENSE).
